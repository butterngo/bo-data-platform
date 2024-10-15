using Bo.Kafka;
using Confluent.Kafka;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Bo.Kafka.Models;

namespace BO.PG.SinkConnector.Handlers;

public class TaskRunPostgresqlHandler : ITaskRunHandler
{
	private readonly IDestinationRepository _destinationRepository;
	private readonly ITaskRunRepository _taskRunRepository;
	private readonly ILogger<TaskRunPostgresqlHandler> _logger;
	private readonly TableRepository _tableRepository;

	private PgAppConfiguration AppConfiguration { get; set; }

	private KafkaConsumer Consumer { get; set; }

	public TaskRunPostgresqlHandler(IDestinationRepository destinationRepository,
		ITaskRunRepository taskRunRepository,
		ILogger<TaskRunPostgresqlHandler> logger,
		TableRepository tableRepository)
	{
		_logger = logger;
		_destinationRepository = destinationRepository;
		_taskRunRepository = taskRunRepository;
		_tableRepository = tableRepository;
	}

	public void Dispose()
	{
		Consumer?.Dispose();
	}

	public async Task HandleAsync(TaskRun state, CancellationToken cancellationToken)
	{
		try
		{
			var destination = await _destinationRepository.GetByAsync(state.ReferenceId);

			if (destination == null)
			{
				throw new InvalidOperationException($"not found referenceId: {state.ReferenceId}");
			}

			AppConfiguration = PgAppConfiguration.Deserialize<PgAppConfiguration>(destination.AppConfiguration);

			_logger.LogInformation($"Starting data with {state.Id}");

			await _taskRunRepository.SetRunningAsync(state.Id, state.RowVersion, cancellationToken);

			_logger.LogInformation($"Runned {state.Id}");

			var kafkaServer = AppConfiguration.Consumer["kafkaServer"].ToString();

			var groupId = AppConfiguration.Consumer["groupId"].ToString();

			_logger.LogDebug($"kafka server {kafkaServer} groupId: {groupId}");

			await _tableRepository.CreateSchemaIfNotExited(AppConfiguration.ConnectionString, AppConfiguration.Schema, cancellationToken);

			Consumer = new KafkaConsumer(new ConsumerConfig
			{
				BootstrapServers = kafkaServer,
				GroupId = groupId,
				PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin,
				AutoOffsetReset = AutoOffsetReset.Earliest,
			});

			if (AppConfiguration.Topics != null)
			{
				await Consumer.Consume(AppConfiguration.Topics, async message =>
				{
					_logger.LogDebug("message: {@message}", message);

					await ExecuteAsync(message, cancellationToken);

				}, cancellationToken);

			}

			if (!string.IsNullOrEmpty(AppConfiguration.TopicPattern))
			{
				await Consumer.Consume(AppConfiguration.TopicPattern, async message =>
				{
					_logger.LogDebug("message: {@message}", message);

					await ExecuteAsync(message, cancellationToken);

				}, cancellationToken);

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

	private async Task ExecuteAsync(KafkaMessage kafkaMessage, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var tableName = _tableRepository.ConvertTableName(kafkaMessage, AppConfiguration.Schema);

		switch (kafkaMessage.op.ToUpper()) 
		{
			case "I": 
				{
					await _tableRepository.InsertAsync(AppConfiguration.ConnectionString, tableName, kafkaMessage, cancellationToken);
					break;
				}
			case "U":
				{
					await _tableRepository.UpdateAsync(AppConfiguration.ConnectionString, tableName, kafkaMessage, cancellationToken);
					break;
				}
			case "D":
				{
					throw new NotImplementedException();
				}
		}
	}
}
