using Bo.Kafka;
using PgOutput2Json;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using BO.PG.SourceConnector.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using BO.Core.Extensions;
using System.Threading;

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

		async Task PublishAsync(PgTableSchema pgtable, string json)
		{
			var topic = string.IsNullOrEmpty(AppConfiguration.Topic) ? pgtable.Topic : AppConfiguration.Topic;

			_logger.LogInformation($"Topic: {topic} table: {pgtable.TableSchame}.{pgtable.TableName}, json: {json}");

			var item = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(json);

			await Producer.ProduceAsync(topic, $"{pgtable.QualifiedName}_{DateTime.Now.Ticks}", pgtable.SerializeKafkaMessage(item), cancellationToken);
		}

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
					try
					{
						await PublishAsync(pgtable, json);
					}
					catch (InvalidOperationException) 
					{
						using var conn = new NpgsqlConnection(AppConfiguration.ConnectionString);

						await conn.OpenAsync(cancellationToken);

						pgtable.ColumnDescriptors = await conn.ExtractColumnAsync(new { table_schema = pgtable.TableSchame, table_name = pgtable.TableName });

						await _sourceRepository.UpdateAppConfigurationAsync(state.ReferenceId, AppConfiguration.Serialize());

						await PublishAsync(pgtable, json);
					}
					catch (Exception ex)
					{
						PgOutput2Json?.Dispose();

						Producer?.Dispose();

						await SetErrorAsync(ex, cancellationToken);
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
