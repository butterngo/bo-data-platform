using Npgsql;

namespace BO.PG.SourceConnector;

internal class PostgresReader
{
	private readonly string _connectionString;

	public PostgresReader(string connectionString)
	{
		_connectionString = connectionString;
	}

	public async IAsyncEnumerable<Dictionary<string, object>> ReadData(string query, CancellationToken cancellationToken)
	{
		var results = new List<Dictionary<string, object>>();

		using (var connection = new NpgsqlConnection(_connectionString))
		{
			await connection.OpenAsync(cancellationToken);
			using (var command = new NpgsqlCommand(query, connection))
			using (var reader = await command.ExecuteReaderAsync(cancellationToken))
			{
				while (reader.Read())
				{
					var row = new Dictionary<string, object>();

					for (int i = 0; i < reader.FieldCount; i++)
					{
						row[reader.GetName(i)] = reader.GetValue(i);
					}

					yield return row;
				}
			}
		}
	}
}
