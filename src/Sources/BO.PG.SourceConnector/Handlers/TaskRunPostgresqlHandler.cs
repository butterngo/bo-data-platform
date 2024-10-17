using Npgsql;
using Bo.Kafka;
using PgOutput2Json;
using Confluent.Kafka;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using BO.PG.SourceConnector.Models;
using Microsoft.Extensions.Logging;
using Bo.Kafka.Models;

namespace BO.PG.SourceConnector.Handlers;

public class TaskRunPostgresqlHandler : TaskRunBaseHandler<TaskRunPostgresqlHandler>
{
    private readonly ISourceRepository _sourceRepository;

	private AvroKafkaProducer Producer { get; set; }

	private PgAppConfiguration AppConfiguration { get; set; }

	private IPgOutput2Json PgOutput2Json { get; set; }

	public TaskRunPostgresqlHandler(ITaskRunRepository taskRunRepository,
		ISourceRepository sourceRepository,
		ILoggerFactory loggerFactory)
		:base(taskRunRepository, loggerFactory)
    {
        _sourceRepository = sourceRepository;
	}

    private async Task FirstLoadAsync(TaskRun state, CancellationToken cancellationToken) 
    {
		var tableSchema = AppConfiguration.Tables.First(x => x.Id == state.Id);

		var conn = new NpgsqlConnection(AppConfiguration.ConnectionString);

		var sql = $"COPY {tableSchema.QualifiedName} ({tableSchema.GetStrColumnName()}) TO STDOUT (FORMAT BINARY)";

		conn.Open();

		using (var reader = conn.BeginBinaryExport(sql))
		{
			while (true)
			{
				if (reader.StartRow() == -1)
				{
					break;
				}

				await Producer.ProduceAsync(tableSchema.Topic, tableSchema.SerializeKafkaMessage(reader, tableSchema.QualifiedName), cancellationToken);
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
			.WithJsonOptions(options =>
			{
				options.WriteTableNames = true;
				options.WriteTimestamps = true;
			})
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

					await Producer.ProduceAsync(topic, pgtable.SerializeKafkaMessage(json), cancellationToken);
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

		_logger.LogInformation($"Runned {state.Id}");
		var kafkaServer = AppConfiguration.Publisher["kafkaServer"].ToString();

		_logger.LogDebug($"kafka server {kafkaServer}");

		var kafkaOptions = new KafkaOptions
		{
			ProducerConfig = new ProducerConfig
			{
				BootstrapServers = kafkaServer
			},
			SchemaRegistryConfig = new Confluent.SchemaRegistry.SchemaRegistryConfig 
			{
				Url = "http://localhost:8081"
			}
		};

		Producer = new AvroKafkaProducer(kafkaOptions);

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
