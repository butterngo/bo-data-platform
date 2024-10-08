using Dapper;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.PG.SourceConnector.Repositories;

internal class SourceRepository : ISourceRepository
{
	private readonly IDataContext _dataContext;
	private readonly ILogger<SourceRepository> _logger;
	private IEnumerable<Source> Entities { get; set; }

	public SourceRepository(IDataContext dataContext, ILogger<SourceRepository> logger)
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