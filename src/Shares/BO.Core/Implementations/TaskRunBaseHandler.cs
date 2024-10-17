using System.Text.Json;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.Core.Implementations;

public abstract class TaskRunBaseHandler<T> : ITaskRunHandler
{
	protected readonly ILoggerFactory _loggerFactory;
	private readonly ITaskRunRepository _taskRunRepository;
	protected readonly ILogger<T> _logger;
	private TaskRun State { get; set; }

	public TaskRunBaseHandler(ITaskRunRepository taskRunRepository, ILoggerFactory loggerFactory)
	{
		_loggerFactory = loggerFactory;
		_logger = loggerFactory.CreateLogger<T>();
		_taskRunRepository = taskRunRepository;
	}

	public async Task HandleAsync(TaskRun state, CancellationToken cancellationToken)
	{
		try
		{
			State = state;

			_logger.LogInformation($"Starting TaskRun {state.Id}");

			await _taskRunRepository.SetRunningAsync(state.Id, state.RowVersion, cancellationToken);

			_logger.LogInformation($"Task {state.Id} is running.");

			await DoWork(state, cancellationToken);

			await OnBeforeCompleting(state, cancellationToken);

			await _taskRunRepository.SetCompletedAsync(state.Id, state.RowVersion, cancellationToken);
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

	protected abstract Task OnBeforeCompleting(TaskRun state, CancellationToken cancellationToken);

	protected abstract Task DoWork(TaskRun state, CancellationToken cancellationToken);

	public void Dispose()
	{
		Dispose(true);
		_taskRunRepository.SetStopAsync(State.Id, State.RowVersion, CancellationToken.None).Wait();
	}

	protected abstract void Dispose(bool isDispose);
}
