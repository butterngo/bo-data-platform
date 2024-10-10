using Bo.Kafka;
using Confluent.Kafka;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace BO.PG.SinkConnector.Handlers;

public class TaskRunPostgresqlHandler : ITaskRunHandler
{
	private readonly IDestinationRepository _destinationRepository;
	private readonly ITaskRunRepository _taskRunRepository;
	private readonly ILogger<TaskRunPostgresqlHandler> _logger;

	private PgAppConfiguration AppConfiguration { get; set; }

	private KafkaConsumer Consumer { get; set; }

	public TaskRunPostgresqlHandler(IDestinationRepository destinationRepository,
		ITaskRunRepository taskRunRepository,
		ILogger<TaskRunPostgresqlHandler> logger) 
	{
		_logger = logger;
		_destinationRepository = destinationRepository;
		_taskRunRepository = taskRunRepository;
	}

	public void Dispose()
	{
		Consumer.Dispose();
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

			Consumer = new KafkaConsumer(new ConsumerConfig
			{
				BootstrapServers = kafkaServer,
				GroupId = groupId,
				PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin,
				AutoOffsetReset = AutoOffsetReset.Earliest,
			});

			if (AppConfiguration.Topics != null)
			{
				Consumer.Consume(AppConfiguration.Topics, async message =>
				{
					_logger.LogInformation("message: {@message}", message);
				}, cancellationToken);

				return;
			}

			if (!string.IsNullOrEmpty(AppConfiguration.TopicPattern))
			{
				Consumer.Consume("bo_connector_northwind_.*", async message =>
				{
					_logger.LogInformation($"message: {@message.ToJsonString()}");
				}, cancellationToken);

				return;
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
}
