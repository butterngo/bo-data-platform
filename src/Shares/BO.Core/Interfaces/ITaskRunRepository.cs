using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface ITaskRunRepository
{
	public Task<int> AddTaskAsync(TaskRun entity, CancellationToken cancellationToken);
	public Task<int> SetRunningAsync(string id, string rowVersion, CancellationToken cancellationToken);
	public Task<int> SetCompletedAsync(string id, string rowVersion, CancellationToken cancellationToken);
	public Task<int> SetStopAsync(string id, string rowVersion, CancellationToken cancellationToken);
	public Task<int> SetErrorAsync(string id, string rowVersion, string error_message, CancellationToken cancellationToken);
	public Task<int> AddTaskAsync(IEnumerable<TaskRun> entities, CancellationToken cancellationToken);
	public Task<IEnumerable<TaskRun>> GetTasksAsync(CancellationToken cancellationToken);
	public Task<TaskRun?> GetTaskByAsync(string id, CancellationToken cancellationToken);
}
