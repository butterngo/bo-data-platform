using Bo.Kafka;
using BO.PG.SourceConnector.Abstractions;
using BO.PG.SourceConnector.Entities;
using BO.PG.SourceConnector.Models;
using BO.PG.SourceConnector.Repositories;
using Confluent.Kafka;
using Npgsql;
using PgOutput2Json;
using System.Text.Json;

namespace BO.PG.SourceConnector.Handlers;

internal class TaskRunKafkaHandler : ITaskRunHandler
{
    private readonly ILogger<TaskRunKafkaHandler> _logger;

    private readonly ITaskRunRepository _taskRunRepository;

    private readonly ISourceRepository _sourceRepository;

	private readonly IPGPublicationAndSlotHandler _pgPublicationAndSlotHandler;

	private KafkaProducer Producer { get; set; }

	private List<PgTableSchema> Tables { get; set; }

	private IPgOutput2Json PgOutput2Json { get; set; }

	private readonly ILoggerFactory _loggerFactory;

	public TaskRunKafkaHandler(ITaskRunRepository taskRunRepository,
		ISourceRepository sourceRepository,
		IPGPublicationAndSlotHandler pgPublicationAndSlotHandler,
		ILoggerFactory loggerFactory)
    {
		_loggerFactory = loggerFactory;
		_logger = loggerFactory.CreateLogger<TaskRunKafkaHandler>();
        _taskRunRepository = taskRunRepository;
        _sourceRepository = sourceRepository;
		_pgPublicationAndSlotHandler = pgPublicationAndSlotHandler;
	}

    private async Task FirstLoadAsync(TaskRun state, CancellationToken cancellationToken) 
    {
		var tableSchema = Tables.First(x => x.Id == state.Id);

		var conn = new NpgsqlConnection(tableSchema.ConnectionString);

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

				await Producer.ProduceAsync(tableSchema.Topic, tableSchema.SerializeKafkaMessage(reader), cancellationToken);
			}
		}
	}

	private async Task ProduceAsync(TaskRun state, Source source, CancellationToken cancellationToken) 
	{
		var tables = string.Join(", ", Tables.Select(x => $"{x.TableSchame}.{x.TableName}"));

		await _pgPublicationAndSlotHandler.HandleAsync(source.ConnectionString,
			source.PgPublicationName,
			source.PgSlotName, tables, cancellationToken);

		var builder = PgOutput2JsonBuilder.Create()
			.WithLoggerFactory(_loggerFactory)
			.WithPgConnectionString(source.ConnectionString)
			.WithPgPublications(source.PgPublicationName)
			.WithPgReplicationSlot(source.PgSlotName)
			.WithJsonOptions(options =>
			{
				options.WriteTimestamps = true;
			})
			.WithMessageHandler(async (json, table, key, partition) =>
			{
				var pgtable = Tables.FirstOrDefault(x => x.QualifiedName.Equals(table));

				if (pgtable == null)
				{
					_logger.LogWarning($"Not found table: {table}");
				}
				else 
				{
					_logger.LogDebug($"Topic: {pgtable.Topic} table: {table}, json: {json}");

					await Producer.ProduceAsync(pgtable.Topic, pgtable.SerializeKafkaMessage(json), cancellationToken);
				}
			});


		PgOutput2Json = builder.Build();

		await PgOutput2Json.Start(cancellationToken);
	}

	public async Task HandleAsync(TaskRun state, CancellationToken cancellationToken)
	{
		try
		{
			var source = await _sourceRepository.GetByAsync(state.ReferenceId);

			if (source == null)
			{
				return;
			}

			Tables = JsonSerializer.Deserialize<List<PgTableSchema>>(source.JsonTable);

			_logger.LogInformation($"Starting cdc data with {state.Id}");

			await _taskRunRepository.SetRunningAsync(state.Id, cancellationToken);

			var dic = JsonSerializer.Deserialize<Dictionary<string, object>>(source.JsonPublisher);

			_logger.LogDebug($"kafka server {source.JsonPublisher}");

			Producer = new KafkaProducer(new ProducerConfig
			{
				BootstrapServers = dic["kafkaServer"].ToString()
			});

			if (state.IsCdcData)
			{
				_logger.LogInformation($"Staring Consume data from Publication: {source.PgPublicationName} and Slot: {source.PgSlotName}");

				await ProduceAsync(state, source, cancellationToken);

				await _taskRunRepository.SetCompletedAsync(state.Id, cancellationToken);
			}
			else
			{
				await FirstLoadAsync(state, cancellationToken);

				await _taskRunRepository.SetCompletedAsync(state.Id, cancellationToken);
			}
		}
		catch (Exception ex) 
		{
			await _taskRunRepository.SetErrorAsync(state.Id, JsonSerializer.Serialize(new
			{
				message = $"{ex.Message}",
				stackTrace = $"{ex.StackTrace}",
			}),cancellationToken);

			_logger.LogError("Message: {@Message} StackTrace: {@StackTrace}", ex.Message, ex.StackTrace);
		}
	}

	public void Dispose()
	{
		PgOutput2Json?.Dispose();

		Producer?.Dispose();
	}
}
