using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface ISrcConnectorMappingHandler<TInput>
{
	Task<(Source Source, IEnumerable<TaskRun> TaskRuns)> HandlAsync(TInput input);
}
