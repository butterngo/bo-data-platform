using BO.Core.Models;
using BO.Core.Entities;
using BO.Core.Interfaces;

namespace BO.Core.Implementations;

public abstract class SourceConnectorMappingBaseHandler<TInput> : ISrcConnectorMappingHandler
	where TInput : ConnectorBaseModel 
{
	public abstract Task<(Source Source, IEnumerable<TaskRun> TaskRuns)> HandlAsync(TInput input, CancellationToken cancellationToken);
}
