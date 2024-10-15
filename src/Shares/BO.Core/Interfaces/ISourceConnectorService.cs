using BO.Core.Models;
using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface ISourceConnectorService
{
	Task<Source> CreateAsync<TInput>(TInput input, CancellationToken cancellationToken) where TInput : ConnectorBaseModel;
}
