
using Avro;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Bo.Kafka.Models;

public record struct KafkaMessageField(string field, string type, bool isPrimary = false, bool option= false);

public record struct KafkaMessageSchema(string name, List<KafkaMessageField> fields) 
{
	public bool IsPrimary(string field) 
	{
		var item = fields.FirstOrDefault(x => x.field.Equals(field));

		return item.isPrimary;
	}

	 //return type switch
							//{
							//	"null" => new PrimitiveSchema(Type.Null, props),
							//	"boolean" => new PrimitiveSchema(Type.Boolean, props),
							//	"int" => new PrimitiveSchema(Type.Int, props),
							//	"long" => new PrimitiveSchema(Type.Long, props),
							//	"float" => new PrimitiveSchema(Type.Float, props),
							//	"double" => new PrimitiveSchema(Type.Double, props),
							//	"bytes" => new PrimitiveSchema(Type.Bytes, props),
							//	"string" => new PrimitiveSchema(Type.String, props),
							//	_ => null,
							//};
	public RecordSchema GenerateAvroSchema()
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
		writer.WriteString("name", "test");
		writer.WritePropertyName("fields");
		writer.WriteStartArray();
		foreach (var field in fields)
		{
			writer.WriteStartObject();
			writer.WriteString("name", field.field);
			if (!field.option)
			{
				writer.WriteString("type", field.type);
			}
			else 
			{
				writer.WritePropertyName("type");
				writer.WriteStartArray();
				writer.WriteStringValue("null");
				writer.WriteStringValue(field.type);
				writer.WriteEndArray();
			}
			
			writer.WriteBoolean("isPrimary", field.isPrimary);
			writer.WriteEndObject();
		}
		writer.WriteEndArray();
		writer.WriteEndObject();
		writer.Flush();

		string json = Encoding.UTF8.GetString(stream.ToArray());

		var avroSchema = (RecordSchema)RecordSchema.Parse(json);

		return avroSchema;
	}
}

public class KafkaMessageGenerator
{
	public KafkaMessageSchema schema { get; set; } = new KafkaMessageSchema("struct", new List<KafkaMessageField>());
	public Dictionary<string, object> payload { get; set; } = new();
	public Dictionary<string, object>? source { get; set; }

	public string op { get; set; } = "I";

	public long ts_ms { get; set; } = DateTime.UtcNow.Ticks;

	public void SetValue(string field, string type, object value, bool isPrimary, bool option) 
	{
		if (!payload.ContainsKey(field))
		{
			schema.fields.Add(new KafkaMessageField(field, type, isPrimary, option));
			payload.Add(field, value);
		}
	}

	public string ToJsonString() => JsonSerializer.Serialize(this);
}
