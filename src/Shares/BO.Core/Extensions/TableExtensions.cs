using Dapper;
using System.Data;
using BO.Core.Models;
using System.Text.Json;
using System.Text.Json.Nodes;


namespace BO.Core.Extensions;

public static class TableExtensions
{
	public static async Task<IEnumerable<ColumnDescriptor>> ExtractColumnAsync(this IDbConnection conn, string sql, object @param) 
	{
		var columns = await conn.QueryAsync<dynamic>(sql, @param);

		var primaryKey = await conn.ExtractPrimaryColumnAsync(@param);

		return columns.Select(obj =>
		{
			const string columnName = "column_name";

			const string dataType = "data_type";

			const string is_nullable = "is_nullable";

			var dictionary = (IDictionary<string, object>)obj;

			return new ColumnDescriptor
			{
				Field = dictionary[columnName].ToString(),
				IsPrimary = dictionary[columnName].ToString().Equals(primaryKey),
				Type = dictionary[dataType].ToString(),
				IsNullable = dictionary[is_nullable].ToString() == "YES" ? true : false,
			};
		});
	}

	public static async Task<IEnumerable<ColumnDescriptor>> ExtractColumnAsync(this IDbConnection conn, object @param)
	{
		var sql = @"select column_name, data_type, is_nullable
			 from INFORMATION_SCHEMA.COLUMNS where table_schema = @table_schema and table_name = @table_name";

		return await ExtractColumnAsync(conn, sql, @param);
	}

	public static async Task<string?> ExtractPrimaryColumnAsync(this IDbConnection conn, object @param)
	{
		var sql = @"SELECT
    kcu.column_name
FROM
    information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
WHERE
    tc.constraint_type = 'PRIMARY KEY'
    and tc.table_schema  = @table_schema
    AND tc.table_name = @table_name;";

		return await conn.QueryFirstOrDefaultAsync<string>(sql, @param);
	}
}
