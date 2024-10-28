using Avro.Generic;
using Newtonsoft.Json.Bson;
using Npgsql;
using Paillave.Etl.Core;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using static BO.PG.SinkConnector.ValuesProviders.PostgreSqlExtensions;

namespace BO.PG.SinkConnector.ValuesProviders;

public class SaveOptions<T>
{
	public string TableName { get; set; }
	public Func<T, object> KeySelector { get; set; }
	public List<string> ColumnsToIgnore { get; set; } = new List<string>();

	public SaveOptions<T> ToTable(string tableName)
	{
		TableName = tableName;
		return this;
	}

	public SaveOptions<T> SeekOn(Func<T, object> keySelector)
	{
		KeySelector = keySelector;
		return this;
	}

	public SaveOptions<T> DoNotSave(params Expression<Func<T, object>>[] columns)
	{
		foreach (var column in columns)
		{
			var member = (MemberExpression)column.Body;
			ColumnsToIgnore.Add(member.Member.Name);
		}
		return this;
	}
}

public class SourceOptions
{
	public string TableName { get; set; }
	public string ConnectionString { get; set; }
	public List<string> Columns { get; set; } = new List<string>();

	public SourceOptions FromConnectionString(string connectionString)
	{
		ConnectionString = connectionString;
		return this;
	}

	public SourceOptions FromTable(string tableName)
	{
		TableName = tableName;
		return this;
	}

	public SourceOptions SelectColumns(params string[] columns)
	{
		Columns.AddRange(columns);
		return this;
	}

	public string ToSqlScript()
	{
		return $"SELECT {string.Join(", ", Columns)} FROM {TableName}";
	}
	public IValuesProvider<string, Dictionary<string, object>> ValuesProvider 
	{
		get { return GetValuesProvider(); }
	}

	private IValuesProvider<string, Dictionary<string, object>> GetValuesProvider() 
	{
		return SimpleValuesProvider.Create<string, Dictionary<string, object>>(async (ctx, dependencyResolver, cancellationToken, push) =>
		{
			var postgresReader = new PostgresReader("Host=localhost:5432;Database=bo_data_platform_docker;Username=postgres;Password=password");

			await foreach (var item in postgresReader.ReadData(ToSqlScript(), cancellationToken))
			{
				push(item);
			}
		});
	}
}

internal static class PostgreSqlExtensions
{
	public static IStream<TOut> CustomPostgreSqlSave<TIn, TOut>(this IStream<TIn> stream, string name, Func<SaveOptions<TIn>, SaveOptions<TOut>> options)
	{
		// Implement your custom PostgreSQL save logic here
		throw new NotImplementedException();
	}

	public static IStream<Dictionary<string, object>> PostgreSqlSource(this IStream<string> stream, string name, Func<SourceOptions, SourceOptions> options)
	{
		var valuesProvider = options.Invoke(new SourceOptions()).ValuesProvider;

		return stream.CrossApply(name, valuesProvider);
	}

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

}
