using System.Text.Json;
using System.Threading;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.Core.Implementations;

public record class ExceptionDetails(string Message, string StackTrace, string InnerException)
{
	public static ExceptionDetails Create(Exception ex) => new ExceptionDetails(ex.Message, ex.StackTrace?.ToString(), ex.InnerException?.ToString());
	public override string ToString()
	{
		return JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
	}
}
public abstract class TaskRunBaseHandler<T> : ITaskRunHandler
{
	protected readonly ILoggerFactory _loggerFactory;
	private readonly ITaskRunRepository _taskRunRepository;
	protected readonly ILogger<T> _logger;
	private ExceptionDetails? ExceptionDetails { get; set; }

	private TaskRun? State { get; set; }

	public TaskRunBaseHandler(ITaskRunRepository taskRunRepository, ILoggerFactory loggerFactory)
	{
		_loggerFactory = loggerFactory;
		_logger = loggerFactory.CreateLogger<T>();
		_taskRunRepository = taskRunRepository;
	}

	protected async Task SetErrorAsync(Exception ex, CancellationToken cancellationToken) 
	{
		var exceptionDetails = ExceptionDetails.Create(ex);
		await _taskRunRepository.SetErrorAsync(State.Id, State.RowVersion, exceptionDetails.ToString(), cancellationToken);
	}

    public async Task HandleAsync(TaskRun taskRun, CancellationToken cancellationToken)
	{
		try
		{
			_logger.LogInformation($"Starting TaskRun {taskRun.Id}");

			State = await _taskRunRepository.SetRunningAsync(taskRun.Id, taskRun.RowVersion, cancellationToken);

			if (State == null) 
			{
				throw new InvalidOperationException($"TaskRun is running to another process Id: {taskRun.Id} RowVersion: {taskRun.RowVersion}");
			}

			_logger.LogInformation($"TaskRun {State.Id} is running.");

			await DoWork(State, cancellationToken);

			await OnBeforeCompleting(State, cancellationToken);

			await _taskRunRepository.SetCompletedAsync(State.Id, State.RowVersion, cancellationToken);
		}
		catch (Exception ex)
		{
			await SetErrorAsync(ex, cancellationToken);
			throw;
		}
	}

	protected abstract Task OnBeforeCompleting(TaskRun state, CancellationToken cancellationToken);

	protected abstract Task DoWork(TaskRun state, CancellationToken cancellationToken);

	public void Dispose()
	{
		Dispose(true);
	}

	protected abstract void Dispose(bool isDispose);
}
