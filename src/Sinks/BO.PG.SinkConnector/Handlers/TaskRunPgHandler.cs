using Bo.Kafka;
using Confluent.Kafka;
using BO.Core.Entities;
using BO.Worker.Models;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.PG.SinkConnector;

public class TaskRunPgHandler : ITaskRunHandler
{
	private readonly IDestinationRepository _destinationRepository;
	private readonly ITaskRunRepository _taskRunRepository;
	private readonly ILogger<TaskRunPgHandler> _logger;

	private PgAppConfiguration AppConfiguration { get; set; }

	private KafkaConsumer Consumer { get; set; }

	public TaskRunPgHandler(IDestinationRepository destinationRepository,
		ITaskRunRepository taskRunRepository,
		ILogger<TaskRunPgHandler> logger) 
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
		var destination = await _destinationRepository.GetByAsync(state.ReferenceId);

		if (destination == null)
		{
			return;
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
			AutoOffsetReset = AutoOffsetReset.Earliest
		});

		Consumer.Consume(AppConfiguration.Topics, async message => 
		{
			_logger.LogInformation("message: {@message}", message);
		}, cancellationToken);
	}
}
