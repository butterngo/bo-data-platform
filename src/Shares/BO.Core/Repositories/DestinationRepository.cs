using Dapper;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.Core.Repositories;

internal class DestinationRepository : IDestinationRepository
{
	private readonly IDataContext _dataContext;
	private readonly ILogger<DestinationRepository> _logger;
	private IEnumerable<Destination> Entities { get; set; }

	public DestinationRepository(IDataContext dataContext, ILogger<DestinationRepository> logger)
	{
		_logger = logger;
		_dataContext = dataContext;
	}

	public async Task<int> CreateAsync(Destination entity)
	{
		_logger.LogInformation("Insert Source to database {@enity}", entity);

		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(Destination.Insert, entity);
	}

	public async Task<int> CreateAsync(List<Destination> entities)
	{
		_logger.LogInformation("Insert Source to database {@entities}", entities);

		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(Destination.Insert, entities);
	}

	public async Task<IEnumerable<Destination>> GetAllAsync()
	{
		if (Entities == null)
		{
			using var conn = _dataContext.CreateConnection();

			Entities = await conn.QueryAsync<Destination>(@"select * from sources");
		}

		return Entities;
	}

	public async Task<Destination?> GetByAsync(string id)
	{
		if (Entities == null)
		{
			Entities = await GetAllAsync();
		}

		return Entities.FirstOrDefault(x => x.Id.Equals(id));
	}
}
