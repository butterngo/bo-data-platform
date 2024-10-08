using BO.Core;
using BO.Core.Entities;
using BO.Core.Extensions;
using BO.Core.Implementations;
using BO.Core.Interfaces;
using BO.PG.SourceConnector.Models;
using System.Text.Json;

namespace BO.PG.SourceConnector.Handlers;

public class SrcConnectorMappingHandler : ISrcConnectorMappingHandler<CreatePGSrcConnector>
{
	public async Task<(Source Source, IEnumerable<TaskRun> TaskRuns)> HandlAsync(CreatePGSrcConnector input)
	{
		var dataContext = new DataContext(input.ConnectionString);

		using var conn = dataContext.CreateConnection();

		var tables = new List<PgTableSchema>();

		var hashKey = $"{input.Schema}_{input.Tables}".ToSha256Hash(10);

		var source = new Source
		{
			Name = hashKey
		};

		var appConfiguration = new PgAppConfiguration
		{
			ConnectionString = input.ConnectionString,
			Publisher = input.Publisher,
			PublicationName = $"BO_PG_SourceConnector_pub_{hashKey}",
			SlotName = $"BO_PG_SourceConnector_slot_{hashKey}_{DateTime.Now.Ticks}",
		};

		var taskRuns = new List<TaskRun>
	{
		new TaskRun
		{
			Id = source.Id,
			ReferenceId = source.Id,
			Name = $"cdc data {input.Tables}",
			AppName = Constants.AppNames.POSTGRESQL,
			IsCdcData = true
		}
	};

		foreach (var table in input.Tables.Split(","))
		{
			var columns = await conn.ExtractColumnAsync(new { table_schema = input.Schema, table_name = table });

			var pgTable = new PgTableSchema(input.Schema, table, columns);

			appConfiguration.Tables.Add(pgTable);

			taskRuns.Add(new TaskRun
			{
				Id = pgTable.Id,
				Name = $"First load: {table}",
				AppName = Constants.AppNames.POSTGRESQL,
				ReferenceId = source.Id,
			});

		}

		source.AppConfiguration = appConfiguration.Serialize();

		return (source, taskRuns);
	}
}
