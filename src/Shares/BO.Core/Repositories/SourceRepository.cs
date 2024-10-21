using Dapper;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.PG.SourceConnector.Repositories;

internal class SourceRepository : ISourceRepository
{
	private readonly IDataContext _dataContext;
	private readonly ILogger<SourceRepository> _logger;
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
		using var conn = _dataContext.CreateConnection();

		return await conn.QueryAsync<Source>(@$"select * from {SourceSchema.Table}");
	}

	public async Task<Source?> GetByAsync(string id)
	{
		using var conn = _dataContext.CreateConnection();

		return await conn.QueryFirstOrDefaultAsync<Source>(@$"select * from {SourceSchema.Table} where id = @id", new { id});
	}

	public async Task<int> UpdateAppConfigurationAsync(string id, string appConfiguration)
	{
		using var conn = _dataContext.CreateConnection();

		return await conn.ExecuteAsync(@$"UPDATE {SourceSchema.Table} SET {SourceSchema.Columns.AppConfiguration}=@appConfiguration where id = @id", new { id, appConfiguration });
	}
}