using BO.PG.SourceConnector.Entities;
using BO.PG.SourceConnector.Entities.Enums;
using Dapper;

namespace BO.PG.SourceConnector.Repositories;

public interface ITaskRunRepository 
{
	public Task<int> AddTaskAsync(TaskRun entity, CancellationToken cancellationToken);
	public Task<int> SetRunningAsync(string id, CancellationToken cancellationToken);
	public Task<int> SetCompletedAsync(string id, CancellationToken cancellationToken);
	public Task<int> SetErrorAsync(string id, string error_message, CancellationToken cancellationToken);
	public Task<int> AddTaskAsync(IEnumerable<TaskRun> entities, CancellationToken cancellationToken);
	public Task<IEnumerable<TaskRun>> GetSourcesAsync(CancellationToken cancellationToken);
	public Task<TaskRun?> GetSourceByAsync(string id, CancellationToken cancellationToken);
}

internal class TaskRunRepository : ITaskRunRepository
{
	private readonly DataContext _dataContext;
	private readonly ILogger<TaskRunRepository> _logger;
	public TaskRunRepository(DataContext dataContext, ILogger<TaskRunRepository> logger)
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

	public async Task<IEnumerable<TaskRun>> GetSourcesAsync(CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		using var conn = _dataContext.CreateConnection();

		return await conn.QueryAsync<TaskRun>($@"select * from task_runs where type = @type and status = @status", new 
		{ 
			type = TaskRunType.Src,
			status = TaskRunStatus.Created
		});
	}

	public async Task<TaskRun?> GetSourceByAsync(string id, CancellationToken cancellationToken)
	{
		using var conn = _dataContext.CreateConnection();

		return await conn.QueryFirstOrDefaultAsync<TaskRun>($@"select * from task_runs where id=@id and type = @type and status = @status", new 
		{ 
			id, 
			type = TaskRunType.Src,
			status = TaskRunStatus.Created
		});
	}

	public async Task<int> SetRunningAsync(string id, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var sql = @"UPDATE task_runs
	SET runned_at= NOW(), status=@status
	WHERE id=@id";
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(sql, new { id ,status = TaskRunStatus.Running });
	}

	public async Task<int> SetCompletedAsync(string id, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var sql = @"UPDATE task_runs
	SET completed_at= NOW(), status=@status
	WHERE id=@id";
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(sql, new { id, status = TaskRunStatus.Completed });
	}

	public async Task<int> SetErrorAsync(string id, string error_message, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var sql = @"UPDATE task_runs
	SET occurred_at= NOW(), status=@status, error_message=@errorMessage
	WHERE id=@id";
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(sql, new { id, status = TaskRunStatus.Errored, errorMessage = error_message });
	}
}
