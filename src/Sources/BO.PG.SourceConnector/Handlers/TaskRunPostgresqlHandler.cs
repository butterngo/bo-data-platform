using Bo.Kafka;
using PgOutput2Json;
using Avro.Generic;
using Confluent.Kafka;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using BO.PG.SourceConnector.Models;
using Microsoft.Extensions.Logging;

namespace BO.PG.SourceConnector.Handlers;

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
		var tableSchema = AppConfiguration.Tables.First(x => x.Id == state.Id);

		var postgresReader = new PostgresReader(AppConfiguration.ConnectionString);

		var sql = $@"select * from {tableSchema.QualifiedName}";

		await foreach (var item in postgresReader.ReadData(sql, cancellationToken)) 
		{
			try
			{
				item.Add("_ct", "I");

				await Producer.ProduceAsync(tableSchema.Topic, $"{tableSchema.QualifiedName}_{DateTime.Now.Ticks}", tableSchema.SerializeKafkaMessage(item), cancellationToken);
			}
			catch (ProduceException<string, GenericRecord> ex)
			{
				_logger.LogError($"Message: {ex.Message} StackTrace: {ex.StackTrace}");
			}
		}
	}

	private async Task ProduceAsync(TaskRun state, CancellationToken cancellationToken) 
	{
		var tables = string.Join(", ", AppConfiguration.Tables.Select(x => $"{x.TableSchame}.{x.TableName}"));

		await new PGPublicationAndSlotHandler(_loggerFactory.CreateLogger<PGPublicationAndSlotHandler>())
			.HandleAsync(AppConfiguration.ConnectionString,
			AppConfiguration.PublicationName,
			AppConfiguration.SlotName, tables, cancellationToken);

		var builder = PgOutput2JsonBuilder.Create()
			.WithLoggerFactory(_loggerFactory)
			.WithPgConnectionString(AppConfiguration.ConnectionString)
			.WithPgPublications(AppConfiguration.PublicationName)
			.WithPgReplicationSlot(AppConfiguration.SlotName)
			.WithMessageHandler(async (json, table, key, partition) =>
			{
				var pgtable = AppConfiguration.Tables.FirstOrDefault(x => x.QualifiedName.Equals(table));

				if (pgtable == null)
				{
					_logger.LogWarning($"Not found table: {table}");
				}
				else 
				{
					var topic = string.IsNullOrEmpty(AppConfiguration.Topic) ? pgtable.Topic : AppConfiguration.Topic;

					_logger.LogInformation($"Topic: {topic} table: {table}, json: {json}");

					try 
					{
						var item = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(json);

						await Producer.ProduceAsync(topic, $"{pgtable.QualifiedName}_{DateTime.Now.Ticks}", pgtable.SerializeKafkaMessage(item), cancellationToken);
					}
					catch (ProduceException<string, GenericRecord> ex)
					{
						_logger.LogError($"Message: {ex.Message} StackTrace: {ex.StackTrace}");
					}
				}
			});

		PgOutput2Json = builder.Build();

		await PgOutput2Json.Start(cancellationToken);
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

			await ProduceAsync(state, cancellationToken);
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
