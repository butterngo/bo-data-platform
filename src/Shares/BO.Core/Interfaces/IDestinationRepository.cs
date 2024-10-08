using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface IDestinationRepository
{
	public Task<int> CreateAsync(Destination entity);

	public Task<int> CreateAsync(List<Destination> entities);

	public Task<IEnumerable<Destination>> GetAllAsync();

	public Task<Destination?> GetByAsync(string id);
}
