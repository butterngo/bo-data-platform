using Dapper;
using NpgsqlTypes;
using System.Text;
using System.Data;
using BO.Core.Models;
using System.Text.Json;
using BO.Core.Converters;
using System.Text.Encodings.Web;

namespace BO.Core.Extensions;

public static class PgTableExtensions
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

	public static string ConvertPgTableToAvroSchema(this TableSchemaBase table, Action<List<ColumnDescriptor>> action = null, string @namespace = null)
	{
		var options = new JsonWriterOptions
		{
			Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
			Indented = true
		};

		var columnDescriptors = table.ColumnDescriptors.ToList();

		action?.Invoke(columnDescriptors);

		using var stream = new MemoryStream();
		using var writer = new Utf8JsonWriter(stream, options);
		writer.WriteStartObject();
		writer.WriteString("type", "record");
		writer.WriteString("name", table.QualifiedName);
		if (!string.IsNullOrEmpty(@namespace))
		{
			writer.WriteString("namespace", @namespace);
		}
		writer.WritePropertyName("fields");
		writer.WriteStartArray();
		foreach (var column in columnDescriptors)
		{
			WriteObject(writer, column);
		}
		writer.WriteEndArray();
		writer.WriteEndObject();
		writer.Flush();

		return Encoding.UTF8.GetString(stream.ToArray());
	}

	public static string GenerateCreateTableScript(TableSchemaBase table)
	{
		var columns = new List<string>();

		foreach (var column in table.ColumnDescriptors)
		{
			columns.Add($"{column.Field} {GetNpgsqlDbType(column.Type)}");
		}

		string columnsDefinition = string.Join(",\n    ", columns);

		return $"CREATE TABLE IF NOT EXISTS {table.TableName} (\n {columnsDefinition}\n);";
	}

	private static IEnumerable<ColumnDescriptor> GetDifferenceColumnDescriptors(IEnumerable<ColumnDescriptor> src1, IEnumerable<ColumnDescriptor> src2) 
	{
		return src1.Where(n =>
		{
			return !src2.Any(o => o.Field.Equals(n.Field));
		});
	}

	private static string GetNpgsqlDbType(string type)
	{
		var npgsqlDbType = ParseEnum(type);
		return npgsqlDbType == NpgsqlDbType.Unknown ? "text" : npgsqlDbType.ToString();
	}

	public static string GenerateAlterTableScript(string tableName,
		IEnumerable<ColumnDescriptor> oldColumnDescriptors,
		IEnumerable<ColumnDescriptor> newColumnDescriptor)
	{
		var addedColumns = GetDifferenceColumnDescriptors(newColumnDescriptor, oldColumnDescriptors)
			.Select(x => $"ADD COLUMN {x.Field} {GetNpgsqlDbType(x.Type)}");

		var dropColumns = GetDifferenceColumnDescriptors(oldColumnDescriptors, newColumnDescriptor)
			.Select(x => $"DROP COLUMN IF EXISTS {x.Field}");

		var columns = addedColumns.Concat(dropColumns).ToList();

		var strBuilder = new StringBuilder();

		strBuilder.AppendLine($"ALTER TABLE {tableName}");

		for (var i = 0; i < columns.Count; i++) 
		{
			if (i != 0) 
			{
				strBuilder.Append(",");
			}
			strBuilder.AppendLine(columns[i]);
		}

		strBuilder.Append(";");

		return strBuilder.ToString();
	}


	private static string GetLogicalType(NpgsqlDbType npgsqlDbType)
	{
		switch (npgsqlDbType)
		{
			case NpgsqlDbType.Timestamp:
			case NpgsqlDbType.TimestampTz:
				{
					return "timestamp-millis";
				}
			case NpgsqlDbType.Time:
				{
					return "time-millis";
				}
			case NpgsqlDbType.Date:
				{
					return "date";
				}
			default:
				{
					return string.Empty;
				}
		}
	}

	private static void WriteObject(Utf8JsonWriter writer, ColumnDescriptor column)
	{
		writer.WriteStartObject();
		writer.WriteString("name", column.Field);

		var npgsqlDbType = ParseEnum(column.Type);

		var logicalType = GetLogicalType(npgsqlDbType);

		var type = TypeConverterHelper.ConvertNpgsqlDbTypeToAvroType(npgsqlDbType);

		writer.WritePropertyName("type");

		void WriteType(Utf8JsonWriter writer, string type, string logicalType)
		{
			if (string.IsNullOrEmpty(logicalType))
			{
				writer.WriteStringValue(type);
			}
			else
			{
				writer.WriteStartObject();
				writer.WriteString("type", type);
				writer.WriteString("logicalType", logicalType);
				writer.WriteEndObject();
			}
		}

		if (!column.IsNullable)
		{
			WriteType(writer, type, logicalType);
		}
		else
		{
			writer.WriteStartArray();
			writer.WriteStringValue("null");
			WriteType(writer, type, logicalType);
			writer.WriteEndArray();
			writer.WritePropertyName("default");
			writer.WriteNullValue();
		}

		writer.WriteBoolean("is_primary", column.IsPrimary);

		writer.WriteEndObject();
	}

	private static NpgsqlDbType ParseEnum(string value)
	{
		try
		{
			return (NpgsqlDbType)Enum.Parse(typeof(NpgsqlDbType), value, true);
		}
		catch
		{
			return NpgsqlDbType.Unknown;
		}
	}
}
