using Npgsql;

namespace BO.PG.SinkConnector;

public class TableRepository
{
	private NpgsqlConnection CreateConnection(string connectionString)
	{
		return new NpgsqlConnection(connectionString);
	}

	//public string ConvertTableName(KafkaMessageGenerator kafkaMessage, string tableSchema) 
	//{
	//	if (!kafkaMessage.source.ContainsKey("table"))
	//	{
	//		return string.Empty;
	//	}

	//	var arr = kafkaMessage.source["table"].ToString().Split('.');

	//	return $"{tableSchema}.{arr[1]}";
	//}

	//private (string columns, string values) GenerateColumnsAndValues(Dictionary<string, object> data) 
	//{
	//	var columns = new List<string>();
	//	var values = new List<string>();

	//	foreach (var item in data) 
	//	{
	//		if (item.Value == null)
	//		{
	//			continue;
	//		}
	//		columns.Add(item.Key);
	//		values.Add($"@{item.Key}");
	//	}

	//	return (string.Join(", ", columns), string.Join(", ", values));
	//}

	//public string GenerateInsertScript(string tableName, Dictionary<string, object> data)
	//{
	//	var (columns, values) = GenerateColumnsAndValues(data);

	//	return $"INSERT INTO {tableName} ({columns}) VALUES ({values});";
	//}

	//public string GenerateUpsertScript(string tableName, Dictionary<string, object> data, string conflictColumn)
	//{
	//	var (columns, values) = GenerateColumnsAndValues(data);
	//	var updates = string.Join(", ", data.Keys.Select(k => $"{k} = EXCLUDED.{k}"));

	//	return $@"
	//       INSERT INTO {tableName} ({columns})
	//       VALUES ({values})
	//       ON CONFLICT ({conflictColumn})
	//       DO UPDATE SET {updates};
	//   ";
	//}

	//public string GenerateUpdateScript(string tableName, Dictionary<string, object> data, string condition)
	//{
	//	var updates = string.Join(", ", data.Where(x=>x.Value != null).Select(kv => $"{kv.Key} = @{kv.Key}"));

	//	return $"UPDATE {tableName} SET {updates} WHERE {condition};";
	//}

	//private object FormatValue(object value)
	//{
	//	JsonElement element = ((JsonElement)value);
	//	switch (element.ValueKind)
	//	{
	//		case JsonValueKind.String:
	//			return element.GetString();
	//		case JsonValueKind.Number:
	//			{
	//				try
	//				{
	//					return element.GetInt32();
	//				}
	//				catch 
	//				{
	//					return element.GetDouble();
	//				}
	//			}
	//		case JsonValueKind.True:
	//		case JsonValueKind.False:
	//			return element.GetBoolean();
	//		default:
	//			throw new NotImplementedException("Unknown type");
	//	}
	//}

	//public string GetSqlType(string type)
	//{
	//	return type switch
	//	{
	//		"int64"   => NpgsqlDbType.Bigint.ToString(),
	//		"double"  => NpgsqlDbType.Double.ToString(),
	//		"float"   => NpgsqlDbType.Real.ToString(),
	//		"decimal" => NpgsqlDbType.Numeric.ToString(),
	//		"int32"   => NpgsqlDbType.Smallint.ToString(),
	//		_ => "TEXT"
	//	};
	//}

	//public async Task CreateSchemaIfNotExited(string connectionString, string SchemaName, CancellationToken cancellationToken)
	//{
	//	using var conn = CreateConnection(connectionString);

	//	await conn.OpenAsync(cancellationToken);

	//	await conn.ExecuteAsync($"CREATE SCHEMA IF NOT EXISTS {SchemaName};");
	//}

	//public async Task InsertAsync(string connectionString, string tableName, KafkaMessageGenerator kafkaMessage, CancellationToken cancellationToken) 
	//{
	//	using var conn = CreateConnection(connectionString);

	//	await conn.OpenAsync(cancellationToken);

	//	await conn.ExecuteAsync(GenerateCreateTableScript(tableName, kafkaMessage.schema.fields));

	//	using (var cmd = new NpgsqlCommand(GenerateInsertScript(tableName, kafkaMessage.payload), conn))
	//	{
	//		foreach (var item in kafkaMessage.payload) 
	//		{
	//			if (item.Value == null)
	//			{
	//				continue;
	//			}
	//			cmd.Parameters.AddWithValue(item.Key, FormatValue(item.Value));
	//		}

	//		cmd.ExecuteNonQuery();
	//	}
	//}

	//public async Task UpdateAsync(string connectionString, string tableName, KafkaMessageGenerator kafkaMessage, CancellationToken cancellationToken) 
	//{
	//	using var conn = CreateConnection(connectionString);

	//	await conn.OpenAsync(cancellationToken);

	//	var primaryField = GetPrimaryField(kafkaMessage);

	//	var condition = $"{primaryField.field} = {kafkaMessage.payload[primaryField.field]}";

	//	using (var cmd = new NpgsqlCommand(GenerateUpdateScript(tableName, kafkaMessage.payload, condition), conn))
	//	{
	//		foreach (var item in kafkaMessage.payload)
	//		{
	//			if (item.Value == null)
	//			{
	//				continue;
	//			}

	//			cmd.Parameters.AddWithValue(item.Key, FormatValue(item.Value));
	//		}

	//		cmd.ExecuteNonQuery();
	//	}
	//}

	//public async Task UpsertAsync(string connectionString, string tableName, KafkaMessageGenerator kafkaMessage)
	//{
	//	using var conn = CreateConnection(connectionString);

	//	await conn.OpenAsync();

	//	var primaryField = GetPrimaryField(kafkaMessage);

	//	await conn.ExecuteAsync(GenerateUpsertScript(tableName, kafkaMessage.payload, primaryField.field));
	//}

	//public Task DeleteAsync(string connectionString, KafkaMessageGenerator kafkaMessage) 
	//{
	//	throw new NotImplementedException();
	//}

	//private KafkaMessageField GetPrimaryField(KafkaMessageGenerator kafkaMessage) 
	//{
	//	return kafkaMessage.schema.fields.FirstOrDefault(x => x.isPrimary);
	//}
}
