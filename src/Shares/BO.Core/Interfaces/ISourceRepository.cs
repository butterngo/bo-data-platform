using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface ISourceRepository
{
	public Task<int> CreateAsync(Source entity);

	public Task<int> CreateAsync(List<Source> entities);

	public Task<IEnumerable<Source>> GetAllAsync();

	public Task<Source?> GetByAsync(string id);
}
