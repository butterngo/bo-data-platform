using BO.PG.SourceConnector.Entities;

namespace BO.PG.SourceConnector.Abstractions;

public interface ITaskRunHandler : IDisposable
{
	Task HandleAsync(TaskRun state, CancellationToken cancellationToken);
}
