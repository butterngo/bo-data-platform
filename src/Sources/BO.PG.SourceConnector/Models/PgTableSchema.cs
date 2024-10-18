using Avro;
using NpgsqlTypes;
using System.Text;
using Avro.Generic;
using BO.Core.Models;
using Bo.Kafka.Models;
using System.Text.Json;
using BO.Core.Extensions;
using System.Text.Json.Nodes;
using System.Text.Encodings.Web;
using System.Security.AccessControl;
using Microsoft.Extensions.Primitives;


namespace BO.PG.SourceConnector.Models;

public class PgTableSchema : TableSchemaBase
{ 
	public PgTableSchema() { }

	public PgTableSchema(string tableSchame, string tableName, IEnumerable<ColumnDescriptor> columnDescriptors)
		: base(tableSchame, tableName, columnDescriptors)
	{
	}

	private string _jsonAvroSchema;
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
				//writer.WriteString("namespace", "BO.PG.SourceConnector.Convertor");
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

	private void WriteObject(Utf8JsonWriter writer, ColumnDescriptor column) 
	{
		writer.WriteStartObject();
		writer.WriteString("name", column.Field);

		var npgsqlDbType = ParseEnum(column.Type);

		var type = npgsqlDbType.MapNpgsqlDbTypeToAvroType();

		if (!column.IsNullable)
		{
			writer.WriteString("type", type);
		}
		else
		{
			writer.WritePropertyName("type");
			writer.WriteStartArray();
			writer.WriteStringValue("null");
			writer.WriteStringValue(type);
			writer.WriteEndArray();
			writer.WritePropertyName("default");
			writer.WriteNullValue();
		}

		writer.WriteBoolean("is_primary", column.IsPrimary);

		switch (npgsqlDbType)
		{
			case NpgsqlDbType.Timestamp:
			case NpgsqlDbType.TimestampTz:
				{
					writer.WriteString("logicalType", "timestamp-millis");
					break;
				}
			case NpgsqlDbType.Time:
				{
					writer.WriteString("logicalType", "time-millis");
					break;
				}
			case NpgsqlDbType.Date:
				{
					writer.WriteString("logicalType", "date");
					break;
				}
				
		}
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
		var avroSchema = (RecordSchema)RecordSchema.Parse(JsonAvroSchema);

		var record = new GenericRecord(avroSchema);
		
		record.Add("op", payload["_ct"]);
		record.Add("ts_ms", DateTime.Now.Ticks);

		foreach (var item in payload)
		{
			if (avroSchema.TryGetField(item.Key, out var field))
			{
				record.Add(item.Key, ChangeType(item.Value, field));
			}
			else 
			{
				//TODO Schema changed
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
			type = (field.Schema as UnionSchema).Schemas.Last().Name.MapAvroTypeToCSharpType();
		}
		else 
		{
			type = field.Schema.Name.MapAvroTypeToCSharpType();
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

	public KafkaMessageGenerator SerializeKafkaMessage(string json)
	{
		var jObj = JsonObject.Parse(json);
		var kafkaMessage = new KafkaMessageGenerator
		{
			op = jObj["_ct"].GetValue<string>(),
			ts_ms = Convert.ToInt64(jObj["_mts"].GetValue<string>()),
			source = new Dictionary<string, object>
			{
				{ "table", jObj["_tbl"].GetValue<string>() }
			}
		};

		foreach (var column in ColumnDescriptors)
		{
			switch (ParseEnum(column.Type))
			{
				case NpgsqlDbType.Bigint:
					{
						kafkaMessage.SetValue(column.Field, "long", jObj[column.Field].GetValue<long>(), column.IsPrimary, column.IsNullable);
						break;
					}
				case NpgsqlDbType.Double:
				case NpgsqlDbType.Numeric:
				case NpgsqlDbType.Money:
					{
						kafkaMessage.SetValue(column.Field, "double", jObj[column.Field].GetValue<double>(), column.IsPrimary, column.IsNullable);
						break;
					}
				case NpgsqlDbType.Real:
					{
						kafkaMessage.SetValue(column.Field, "float", jObj[column.Field].GetValue<float>(), column.IsPrimary, column.IsNullable);
						break;
					}
				case NpgsqlDbType.Smallint:
					{
						kafkaMessage.SetValue(column.Field, "int", jObj[column.Field].GetValue<int>(), column.IsPrimary, column.IsNullable);
						break;
					}
				case NpgsqlDbType.Bytea:
					{
						kafkaMessage.SetValue(column.Field, "string", jObj[column.Field].GetValue<string>(), column.IsPrimary, column.IsNullable);
						break;
					}
				default:
					{
						kafkaMessage.SetValue(column.Field, "string", jObj[column.Field].GetValue<string>(), column.IsPrimary, column.IsNullable);
						break;
					}
			}
		}

		return kafkaMessage;
	}
}
