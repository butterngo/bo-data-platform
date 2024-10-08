using BO.Core.Models;
using Dapper;
using System.Data;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Data.SqlTypes;

namespace BO.Core.Extensions;

public static class TableExtensions
{
	private static List<Dictionary<string, string>> Deserialize(string json)
	{
		var columnDescriptors = new List<Dictionary<string, string>>();

		var nodes = JsonNode.Parse(json).AsArray();

		foreach (var node in nodes)
		{
			columnDescriptors.Add(JsonSerializer.Deserialize<Dictionary<string, string>>(node.GetValue<string>()));
		}

		return columnDescriptors;
	}

	public static async Task<IEnumerable<ColumnDescriptor>> ExtractColumnAsync(this IDbConnection conn, string sql, object @param) 
	{
		var columns = await conn.QueryAsync<string>(sql, @param);

		return columns.Select(jsonString =>
		{
			const string columnName = "column_name";

			const string dataType = "data_type";

			const string is_nullable = "is_nullable";

			var jsonObject = JsonNode.Parse(jsonString).AsObject();

			return new ColumnDescriptor
			{
				Field = jsonObject[columnName].ToString(),
				Type = jsonObject[dataType].ToString(),
				IsNullable = jsonObject[is_nullable].ToString() == "YES" ? true : false,
			};
		});
	}

	public static async Task<IEnumerable<ColumnDescriptor>> ExtractColumnAsync(this IDbConnection conn, object @param)
	{
		var sql = @"select row_to_json(table_schema) 
			from ( 
			   select column_name, data_type, is_nullable
			 from INFORMATION_SCHEMA.COLUMNS where table_schema = @table_schema and table_name = @table_name 
			) table_schema";

		return await ExtractColumnAsync(conn, sql, @param);
	}
}
