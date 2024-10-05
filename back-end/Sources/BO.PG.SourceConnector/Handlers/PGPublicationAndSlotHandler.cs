using BO.PG.SourceConnector.Abstractions;
using BO.PG.SourceConnector.Entities;
using Dapper;
using System.Data;
using System.Data.Common;

namespace BO.PG.SourceConnector.Handlers;

internal class PGPublicationAndSlotHandler : IPGPublicationAndSlotHandler
{
	private readonly ILogger<PGPublicationAndSlotHandler> _logger;

	public PGPublicationAndSlotHandler(ILogger<PGPublicationAndSlotHandler> logger) 
	{
		_logger = logger;
	}

	public async Task<(string PublicationName, string SlotName)> HandleAsync(string connectionString,
		string publicationName,
		string slotName,
		string tables, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var dataContext = new DataContext(connectionString);

		using var conn = dataContext.CreateConnection();

		conn.Open();

		_logger.LogInformation($"Starting create Publication {publicationName}");

		await CreatePublication(conn, publicationName, tables);

		await Task.Delay(1000);

		_logger.LogInformation($"Publication {publicationName} created");

		_logger.LogInformation($"Starting create Slot {slotName}");

		await CreateSlot(conn, slotName);

		await Task.Delay(1000);

		_logger.LogInformation($"Slot {slotName} created");

		return (publicationName, slotName);

	}

	private async Task CreatePublication(IDbConnection conn, string name, string tables)
	{
		var publicationExists = await conn.ExecuteScalarAsync<bool>(
				"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = @PublicationName)",
				new { PublicationName = name });

		if (!publicationExists)
		{
			var createPublicationQuery = $"CREATE PUBLICATION {name} FOR TABLE {tables}";
			await conn.ExecuteAsync(createPublicationQuery);
		}
	}

	private async Task CreateSlot(IDbConnection conn, string name) 
	{
		var slotExists = await conn.ExecuteScalarAsync<bool>(
			   "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @SlotName AND plugin = 'pgoutput')",
			   new { SlotName = name });

		// Create the slot if it doesn't exist
		if (!slotExists)
		{
			await conn.ExecuteAsync(
				"SELECT pg_create_logical_replication_slot(@SlotName, 'pgoutput')",
				new { SlotName = name });
		}
	}
}
