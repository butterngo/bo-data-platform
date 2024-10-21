using Avro;
using NpgsqlTypes;
using System.Text;
using Avro.Generic;
using BO.Core.Models;
using System.Text.Json;
using System.Text.Encodings.Web;
using BO.Core.Converters;
using System.Text.Json.Serialization;


namespace BO.PG.SourceConnector.Models;

public class PgTableSchema : TableSchemaBase
{ 
	public PgTableSchema() { }

	public PgTableSchema(string tableSchame, string tableName, IEnumerable<ColumnDescriptor> columnDescriptors)
		: base(tableSchame, tableName, columnDescriptors)
	{
	}

	private string _jsonAvroSchema;

	[JsonIgnore]
	public string JsonAvroSchema 
	{
		get 
		{
			if (string.IsNullOrEmpty(_jsonAvroSchema))
			{
				var options = new JsonWriterOptions
				{
					Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
					Indented = true
				};

				using var stream = new MemoryStream();
				using var writer = new Utf8JsonWriter(stream, options);
				writer.WriteStartObject();
				writer.WriteString("type", "record");
				writer.WriteString("name", QualifiedName);
				writer.WriteString("namespace", "BO.PG.SourceConnector.Convertor");
				writer.WritePropertyName("fields");
				writer.WriteStartArray();
				WriteObject(writer, new ColumnDescriptor { Field = "op", Type = "string" });
				WriteObject(writer, new ColumnDescriptor { Field = "ts_ms", Type = NpgsqlDbType.Timestamp.ToString() });
				foreach (var column in ColumnDescriptors)
				{
					WriteObject(writer, column);
				}
				writer.WriteEndArray();
				writer.WriteEndObject();
				writer.Flush();

				_jsonAvroSchema = Encoding.UTF8.GetString(stream.ToArray());
			}
			return _jsonAvroSchema;
		}
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
	private void WriteObject(Utf8JsonWriter writer, ColumnDescriptor column) 
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

	protected static NpgsqlDbType ParseEnum(string value)
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

	public GenericRecord SerializeKafkaMessage(Dictionary<string, object> payload) 
	{
		const string operation_key = "_ct";
		var avroSchema = (RecordSchema)RecordSchema.Parse(JsonAvroSchema);

		var record = new GenericRecord(avroSchema);

		record.Add("op", payload[operation_key]);
		
		record.Add("ts_ms", DateTime.UtcNow);

		foreach (var item in payload)
		{
			if (item.Key.Equals(operation_key))
			{
				continue;
			}
			if (avroSchema.TryGetField(item.Key, out var field))
			{
				record.Add(item.Key, ChangeType(item.Value, field));
			}
			else 
			{
				throw new InvalidOperationException($"not found field {item.Key} maybe schema changed, please check it.");
			}
		}

		return record;
	}

	private object ChangeType(object value, Field field) 
	{
		bool isUnionSchema = field.Schema is UnionSchema;

		Type type = null;

		if (isUnionSchema)
		{
			type = TypeConverterHelper.ConvertAvroTypeToCSharpType((field.Schema as UnionSchema).Schemas.Last().Name);
		}
		else 
		{
			type = TypeConverterHelper.ConvertAvroTypeToCSharpType(field.Schema.Name);
		}

		try
		{
			return Convert.ChangeType(value, type);
		}
		catch 
		{
			return null;
		}
	}
}
