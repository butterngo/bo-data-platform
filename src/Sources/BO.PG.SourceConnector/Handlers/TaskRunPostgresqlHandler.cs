using Npgsql;
using Bo.Kafka;
using PgOutput2Json;
using BO.Core.Entities;
using BO.Core.Extensions;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using BO.PG.SourceConnector.Models;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using System.Threading.Tasks.Dataflow;
using Avro.Generic;

namespace BO.PG.SourceConnector.Handlers;

internal record CdcDataInput(string json, string table, int partition) 
{
	public string key { get; set; }
}

internal record CdcDataOutput(string topic, string key) 
{
	public GenericRecord payload { get; set; }
}

public class TaskRunPostgresqlHandler : TaskRunBaseHandler<TaskRunPostgresqlHandler>
{
    private readonly ISourceRepository _sourceRepository;

	private IKafkaProducer Producer { get; set; }

	private PgAppConfiguration AppConfiguration { get; set; }

	private IPgOutput2Json PgOutput2Json { get; set; }

	public TaskRunPostgresqlHandler(ITaskRunRepository taskRunRepository,
		ISourceRepository sourceRepository,
		IKafkaProducer producer,
		ILoggerFactory loggerFactory)
		:base(taskRunRepository, loggerFactory)
    {
		Producer = producer;
		_sourceRepository = sourceRepository;
	}
	
	private async Task FirstLoadAsync(TaskRun state, CancellationToken cancellationToken) 
    {
		var pgtable = AppConfiguration.Tables.First(x => x.Id == state.Id);

		var postgresReader = new PostgresReader(AppConfiguration.ConnectionString);

		var sql = $@"select * from {pgtable.QualifiedName}";

		await foreach (var item in postgresReader.ReadData(sql, cancellationToken)) 
		{
			item.Add("_ct", "I");

			await Producer.ProduceAsync(pgtable.Topic, $"{pgtable.QualifiedName}_{DateTime.Now.Ticks}", pgtable.SerializeKafkaMessage(item), cancellationToken);
		}
	}

	private async Task ConsumeAsync(TaskRun state, CancellationToken cancellationToken) 
	{
		var tables = string.Join(", ", AppConfiguration.Tables.Select(x => $"{x.TableSchame}.{x.TableName}"));

		await new PGPublicationAndSlotHandler(_loggerFactory.CreateLogger<PGPublicationAndSlotHandler>())
			.HandleAsync(AppConfiguration.ConnectionString,
			AppConfiguration.PublicationName,
			AppConfiguration.SlotName, tables, cancellationToken);

		var dataflowLinkOptions = new DataflowLinkOptions { PropagateCompletion = true };
		var executionDataflowBlockOptions = new ExecutionDataflowBlockOptions 
		{
			CancellationToken = cancellationToken,
			BoundedCapacity = 100
		};

		var tranformBlock = new TransformBlock<CdcDataInput, CdcDataOutput>(async cdcData => 
		{
			var pgtable = AppConfiguration.Tables.FirstOrDefault(x => x.QualifiedName.Equals(cdcData.table));

			var topic = string.IsNullOrEmpty(AppConfiguration.Topic) ? pgtable.Topic : AppConfiguration.Topic;

			_logger.LogInformation($"Topic: {topic} table: {pgtable.TableSchame}.{pgtable.TableName}, json: {cdcData.json}");
			
			cdcData.key = $"{pgtable.QualifiedName}_{DateTime.Now.Ticks}";

			var item = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(cdcData.json);

			var cdcDataOutput = new CdcDataOutput(topic, cdcData.key);
			try 
			{
				cdcDataOutput.payload = pgtable.SerializeKafkaMessage(item);
			}
			catch (InvalidOperationException)
			{
				using var conn = new NpgsqlConnection(AppConfiguration.ConnectionString);

				await conn.OpenAsync(cancellationToken);

				pgtable.ColumnDescriptors = await conn.ExtractColumnAsync(new { table_schema = pgtable.TableSchame, table_name = pgtable.TableName });

				await _sourceRepository.UpdateAppConfigurationAsync(state.ReferenceId, AppConfiguration.Serialize());

				cdcDataOutput.payload = pgtable.SerializeKafkaMessage(item);
			}

			return cdcDataOutput;
		}, executionDataflowBlockOptions);

		var actionBlock = new ActionBlock<CdcDataOutput>(async item => 
		{
			await Producer.ProduceAsync(item.topic, item.key , item.payload, cancellationToken);
		}, executionDataflowBlockOptions);

		tranformBlock.LinkTo(actionBlock, dataflowLinkOptions);

		var builder = PgOutput2JsonBuilder.Create()
			.WithLoggerFactory(_loggerFactory)
			.WithPgConnectionString(AppConfiguration.ConnectionString)
			.WithPgPublications(AppConfiguration.PublicationName)
			.WithPgReplicationSlot(AppConfiguration.SlotName)
			.WithMessageHandler((json, table, key, partition) => tranformBlock.Post(new CdcDataInput(json, table, partition) { key = key }));

		PgOutput2Json = builder.Build();

		await PgOutput2Json.Start(cancellationToken);

		tranformBlock.Complete();

		await actionBlock.Completion;
	}

	protected override Task OnBeforeCompleting(TaskRun state, CancellationToken cancellationToken) 
		=> Task.CompletedTask;
	
	protected override async Task DoWork(TaskRun state, CancellationToken cancellationToken)
	{
		var source = await _sourceRepository.GetByAsync(state.ReferenceId);

		if (source == null)
		{
			throw new InvalidOperationException($"not found referenceId: {state.ReferenceId}");
		}

		AppConfiguration = PgAppConfiguration.Deserialize<PgAppConfiguration>(source.AppConfiguration);

		if (state.IsCdcData)
		{
			_logger.LogInformation($"Staring Consume data from Publication: {AppConfiguration.PublicationName} and Slot: {AppConfiguration.SlotName}");

			await ConsumeAsync(state, cancellationToken);
		}
		else
		{
			await FirstLoadAsync(state, cancellationToken);
		}
	}

	protected override void Dispose(bool isDispose)
	{
		PgOutput2Json?.Dispose();

		Producer?.Dispose();
	}
}
