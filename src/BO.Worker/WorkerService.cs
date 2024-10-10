using BO.Core.Interfaces;

namespace BO.Worker;

public class WorkerService : BackgroundService
{
	private readonly ILogger<WorkerService> _logger;

	private readonly ILoggerFactory _loggerFactory;

	private readonly IServiceProvider _serviceProvider;

	public WorkerService(ILoggerFactory loggerFactory, IServiceProvider serviceProvider)
	{
		_logger = loggerFactory.CreateLogger<WorkerService>();
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

				var taskRunFactory = _serviceProvider.GetRequiredService<ITaskRunFactory>();

				using var scope = _serviceProvider.CreateScope();

				var taskRunRepository = scope.ServiceProvider.GetRequiredService<ITaskRunRepository>();

				var taskRuns = await taskRunRepository.GetTasksAsync(stoppingToken);

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
							var taskRunHandler = taskRunFactory.GetHander(state.AppName, state.Type);
							await taskRunHandler.HandleAsync(state, token);
						}, stoppingToken);
					}
					catch (ArgumentOutOfRangeException ex)
					{
						_logger.LogWarning(ex.Message);
					}
					catch (Exception ex) 
					{
						_logger.LogError("Message: {@Message} StackTrace: {@StackTrace}", ex.Message, ex.StackTrace);
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
