using Bo.Kafka;
using Confluent.Kafka;
using BO.Core.Entities;
using Bo.Kafka.Models;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using Microsoft.Extensions.Logging;

namespace BO.PG.SinkConnector.Handlers;

public class TaskRunPostgresqlHandler : TaskRunBaseHandler<TaskRunPostgresqlHandler>
{
	private readonly IDestinationRepository _destinationRepository;
	private readonly TableRepository _tableRepository;

	private PgAppConfiguration AppConfiguration { get; set; }

	private KafkaConsumer Consumer { get; set; }

	public TaskRunPostgresqlHandler(IDestinationRepository destinationRepository,
		ITaskRunRepository taskRunRepository,
		ILoggerFactory loggerFactory,
		TableRepository tableRepository)
		:base(taskRunRepository, loggerFactory)
	{
		_tableRepository = tableRepository;
		_destinationRepository = destinationRepository;
	}

	protected override void Dispose(bool isDispose)
	{
		Consumer?.Dispose();
	}

	private async Task ExecuteAsync(KafkaMessageGenerator kafkaMessage, CancellationToken cancellationToken)
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

	protected override Task OnBeforeCompleting(TaskRun state, CancellationToken cancellationToken)
	=> Task.CompletedTask;

	protected override async Task DoWork(TaskRun state, CancellationToken cancellationToken)
	{
		var destination = await _destinationRepository.GetByAsync(state.ReferenceId);

		if (destination == null)
		{
			throw new InvalidOperationException($"not found referenceId: {state.ReferenceId}");
		}

		AppConfiguration = PgAppConfiguration.Deserialize<PgAppConfiguration>(destination.AppConfiguration);

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

				//await ExecuteAsync(message, cancellationToken);

			}, cancellationToken);

		}

		if (!string.IsNullOrEmpty(AppConfiguration.TopicPattern))
		{
			await Consumer.Consume(AppConfiguration.TopicPattern, async message =>
			{
				_logger.LogDebug("message: {@message}", message);

				//await ExecuteAsync(message, cancellationToken);

			}, cancellationToken);
		}
	}
}
