using BO.PG.SourceConnector.Entities;
using Dapper;
using Npgsql;
using NpgsqlTypes;

namespace BO.PG.SourceConnector.Repositories;

public interface ISourceRepository 
{
	public Task<int> CreateAsync(Source entity);

	public Task<int> CreateAsync(List<Source> entities);

	public Task<IEnumerable<Source>> GetAllAsync();

	public Task<Source?> GetByAsync(string id);
}

internal class SourceRepository : ISourceRepository
{
	private readonly DataContext _dataContext;
	private readonly ILogger<SourceRepository> _logger;
	
	private IEnumerable<Source> Entities { get; set; }

	public SourceRepository(DataContext dataContext, ILogger<SourceRepository> logger)
	{
		_logger = logger;
		_dataContext = dataContext;
	}

	public async Task<int> CreateAsync(Source entity)
	{
		_logger.LogInformation("Insert Source to database {@enity}", entity);

		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(Source.Insert, entity);
	}

	public async Task<int> CreateAsync(List<Source> entities)
	{
		_logger.LogInformation("Insert Source to database {@entities}", entities);

		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(Source.Insert, entities);
	}

	public async Task<IEnumerable<Source>> GetAllAsync()
	{
		if (Entities == null)
		{
			using var conn = _dataContext.CreateConnection();

			Entities = await conn.QueryAsync<Source>(@"select * from sources");
		}

		return Entities;
	}

	public async Task<Source?> GetByAsync(string id)
	{
		if (Entities == null)
		{
			Entities = await GetAllAsync();
		}

		return Entities.FirstOrDefault(x=>x.Id.Equals(id));
	}
}
