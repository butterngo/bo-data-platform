using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface ISinkConnectorMappingHandler<TInput>
{
	Task<(Destination Destination, TaskRun TaskRun)> HandlAsync(TInput input);
}
