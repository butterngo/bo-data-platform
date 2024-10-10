﻿using Bo.Kafka;
using Npgsql;
using PgOutput2Json;
using Confluent.Kafka;
using System.Text.Json;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.PG.SourceConnector.Models;
using Microsoft.Extensions.Logging;

namespace BO.PG.SourceConnector.Handlers;

public class TaskRunPostgresqlHandler : ITaskRunHandler
{
    private readonly ILogger<TaskRunPostgresqlHandler> _logger;

    private readonly ITaskRunRepository _taskRunRepository;

    private readonly ISourceRepository _sourceRepository;

	private KafkaProducer Producer { get; set; }

	private PgAppConfiguration AppConfiguration { get; set; }

	private IPgOutput2Json PgOutput2Json { get; set; }

	private readonly ILoggerFactory _loggerFactory;

	public TaskRunPostgresqlHandler(ITaskRunRepository taskRunRepository,
		ISourceRepository sourceRepository,
		ILoggerFactory loggerFactory)
    {
		_loggerFactory = loggerFactory;
		_logger = loggerFactory.CreateLogger<TaskRunPostgresqlHandler>();
        _taskRunRepository = taskRunRepository;
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

	public async Task HandleAsync(TaskRun state, CancellationToken cancellationToken)
	{
		try
		{
			var source = await _sourceRepository.GetByAsync(state.ReferenceId);

			if (source == null)
			{
				throw new InvalidOperationException($"not found referenceId: {state.ReferenceId}");
			}

			AppConfiguration = PgAppConfiguration.Deserialize<PgAppConfiguration>(source.AppConfiguration);

			_logger.LogInformation($"Starting data with {state.Id}");

			await _taskRunRepository.SetRunningAsync(state.Id, state.RowVersion, cancellationToken);

			_logger.LogInformation($"Runned {state.Id}");
			var kafkaServer = AppConfiguration.Publisher["kafkaServer"].ToString();

			_logger.LogDebug($"kafka server {kafkaServer}");

			Producer = new KafkaProducer(new ProducerConfig
			{
				BootstrapServers = kafkaServer
			});

			if (state.IsCdcData)
			{
				_logger.LogInformation($"Staring Consume data from Publication: {AppConfiguration.PublicationName} and Slot: {AppConfiguration.SlotName}");

				await ProduceAsync(state, cancellationToken);

				await _taskRunRepository.SetCompletedAsync(state.Id, state.RowVersion, cancellationToken);
			}
			else
			{
				await FirstLoadAsync(state, cancellationToken);

				await _taskRunRepository.SetCompletedAsync(state.Id, state.RowVersion, cancellationToken);
			}
		}
		catch (Exception ex) 
		{
			await _taskRunRepository.SetErrorAsync(state.Id, state.RowVersion, JsonSerializer.Serialize(new
			{
				message = $"{ex.Message}",
				stackTrace = $"{ex.StackTrace}",
			}), cancellationToken);
			throw;
		}
	}

	public void Dispose()
	{
		PgOutput2Json?.Dispose();

		Producer?.Dispose();
	}
}
