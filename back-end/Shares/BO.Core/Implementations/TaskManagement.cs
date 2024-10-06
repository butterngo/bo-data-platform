using BO.Core.Interfaces;
using BO.Core.Models;
using System.Collections.Concurrent;

namespace BO.Core.TaskManagement;

public class TaskManagement : ITaskManagement
{
	private readonly IServiceProvider _serviceProvider;

	private readonly TaskManagementConfiguration _configuration;

	private readonly ConcurrentDictionary<string, CancellationTokenSource> _tasks;

	private readonly SemaphoreSlim _semaphoreSlim;
	
	private bool IsCompleted { get; set; } = false;

	public TaskManagement(IServiceProvider serviceProvider, TaskManagementConfiguration configuration)
	{
		_configuration = configuration;

		_serviceProvider = serviceProvider;

		_tasks = new ConcurrentDictionary<string, CancellationTokenSource>();

		_semaphoreSlim = new SemaphoreSlim(configuration.MaxParallelism);
	}

	public Task Completion 
	{
		get 
		{
			while (!IsCompleted) 
			{
				Task.Delay(500).Wait();
				break;
			} 

			return Task.CompletedTask;
		}
	}

	public void DoWork<TInput>(TInput model, Func<TInput, IServiceProvider, CancellationToken, Task> func, CancellationToken cancellationToken)
		where TInput : ITaskRun
	{
		if (_configuration.MaxParallelism == _tasks.Count) 
		{
			throw new ArgumentOutOfRangeException($"{_configuration.MaxParallelism} tasks are running at the moment, please extend MaxParallelism in appsetting");
		}

		if (!_tasks.ContainsKey(model.Id)) 
		{
			Task.Run(async () =>
			{
				await _semaphoreSlim.WaitAsync();

				CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

				if (_tasks.TryAdd(model.Id, cancellationTokenSource)) 
				{
					try
					{
						await func(model, _serviceProvider, cancellationTokenSource.Token);
					}
					finally
					{
						_semaphoreSlim.Release();
						_tasks.Remove(model.Id, out var _);
					}
				}

			}, cancellationToken);
		}
	}

	public void Cancel(string taskId) 
	{
		if (_tasks.ContainsKey(taskId)) 
		{
			_tasks[taskId].Cancel();
		}
	}

	public void Dispose()
	{
		foreach (var task in _tasks.Values) 
		{
			task.Cancel();
		}

		IsCompleted = true;
	}
}
