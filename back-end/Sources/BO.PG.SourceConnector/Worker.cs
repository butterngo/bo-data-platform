﻿using BO.PG.SourceConnector.Abstractions;
using BO.PG.SourceConnector.Repositories;

namespace BO.PG.SourceConnector;

public class Worker : BackgroundService
{
	private readonly ILogger<Worker> _logger;

	private readonly ILoggerFactory _loggerFactory;

	private readonly IServiceProvider _serviceProvider;

	public Worker(ILoggerFactory loggerFactory, IServiceProvider serviceProvider) 
	{
		_logger = loggerFactory.CreateLogger<Worker>();
		_serviceProvider = serviceProvider;
		_loggerFactory = loggerFactory;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		while (!stoppingToken.IsCancellationRequested) 
		{
			try
			{
				var taskManagement = _serviceProvider.GetRequiredService<ITaskManagement>();

				using var scope = _serviceProvider.CreateScope();

				var taskRunRepository = scope.ServiceProvider.GetRequiredService<ITaskRunRepository>();

				var taskRuns = await taskRunRepository.GetSourcesAsync(stoppingToken);

				if (taskRuns.Count() == 0) 
				{
					_logger.LogDebug($"Not found any 'Created TaskRun' {DateTime.Now}");
				}
				foreach (var taskRun in taskRuns)
				{
					try
					{
						taskManagement.DoWork(taskRun, async (state, provider, token) =>
						{
							using var scope = _serviceProvider.CreateScope();
							var taskRunRepository = scope.ServiceProvider.GetRequiredService<ITaskRunRepository>();
							var sourceRepository = scope.ServiceProvider.GetRequiredService<ISourceRepository>();
							var taskRunKafkaHandler = scope.ServiceProvider.GetRequiredService<ITaskRunHandler>();
							await taskRunKafkaHandler.HandleAsync(state, token);

						}, stoppingToken);
					}
					catch (ArgumentOutOfRangeException ex)
					{
						_logger.LogWarning(ex.Message);
					}
				}
			}
			catch (Exception ex)
			{
				_logger.LogError("Message: {@Message} StackTrace: {@StackTrace}", ex.Message, ex.StackTrace);
			}

			await Task.Delay(5000);
		}
	}

	public override async Task StopAsync(CancellationToken cancellationToken)
	{
		var taskManagement = _serviceProvider.GetRequiredService<ITaskManagement>();

		taskManagement.Dispose();

		await taskManagement.Completion;

		await base.StopAsync(cancellationToken);
	}
}
