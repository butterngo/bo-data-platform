using BO.Core.Entities;
using BO.Core.Models;

namespace BO.Core.Interfaces;

public interface IDestinationConnectorService
{
	Task<Destination> CreateAsync<TInput>(TInput input, CancellationToken cancellationToken) where TInput : ConnectorBaseModel;
}
