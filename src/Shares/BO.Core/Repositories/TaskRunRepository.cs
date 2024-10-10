using Dapper;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.Core.Entities.Enums;
using Microsoft.Extensions.Logging;

namespace BO.PG.SourceConnector.Repositories;

internal class TaskRunRepository : ITaskRunRepository
{
	private readonly IDataContext _dataContext;
	private readonly ILogger<TaskRunRepository> _logger;
	public TaskRunRepository(IDataContext dataContext, ILogger<TaskRunRepository> logger)
	{
		_logger = logger;
		_dataContext = dataContext;
	}

	public async Task<int> AddTaskAsync(TaskRun entity,CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		_logger.LogInformation("AddTask to database {@entity}", entity);

		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(TaskRun.Insert, entity);
	}

	public async Task<int> AddTaskAsync(IEnumerable<TaskRun> entities, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		_logger.LogInformation("AddTask to database {@entity}", entities);

		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(TaskRun.Insert, entities);
	}

	public async Task<IEnumerable<TaskRun>> GetTasksAsync(CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		using var conn = _dataContext.CreateConnection();

		return await conn.QueryAsync<TaskRun>($@"select * from task_runs where status = @status", new 
		{ 
			status = TaskRunStatus.Created
		});
	}

	public async Task<TaskRun?> GetTaskByAsync(string id, CancellationToken cancellationToken)
	{
		using var conn = _dataContext.CreateConnection();

		return await conn.QueryFirstOrDefaultAsync<TaskRun>($@"select * from task_runs where id=@id and status = @status", new 
		{ 
			id, 
			status = TaskRunStatus.Created
		});
	}

	public async Task<int> SetRunningAsync(string id, string rowVersion, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var sql = @"UPDATE task_runs
	SET runned_at= NOW(), status=@status, row_version=gen_random_uuid()
	WHERE id=@id and row_version=@rowVersion";
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(sql, new
		{
			id,
			status = TaskRunStatus.Running,
			rowVersion
		});
	}

	public async Task<int> SetCompletedAsync(string id, string rowVersion, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var sql = @"UPDATE task_runs
	SET completed_at= NOW(), status=@status, row_version=gen_random_uuid()
	WHERE id=@id and row_version=@rowVersion";
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(sql, new { id, status = TaskRunStatus.Completed, rowVersion });
	}

	public async Task<int> SetErrorAsync(string id, string rowVersion, string error_message, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var sql = @"UPDATE task_runs
	SET occurred_at= NOW(), status=@status, error_message=@errorMessage, row_version=gen_random_uuid()
	WHERE id=@id and row_version=@rowVersion";
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(sql, new { id, status = TaskRunStatus.Errored, errorMessage = error_message, rowVersion });
	}
}