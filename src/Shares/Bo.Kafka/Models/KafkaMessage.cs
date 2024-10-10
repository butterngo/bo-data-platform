
using System.Text.Json;

namespace Bo.Kafka.Models;

public record struct KafkaMessageField(string type, bool isPrimary, bool option, string field);

public record struct KafkaMessageSchema(string type, List<KafkaMessageField> fields) 
{
	public bool IsPrimary(string field) 
	{
		var item = fields.FirstOrDefault(x => x.field.Equals(field));

		return item.isPrimary;
	}
}

public class KafkaMessage
{
	public KafkaMessageSchema schema { get; set; } = new KafkaMessageSchema("struct", new List<KafkaMessageField>());
	public Dictionary<string, object> payload { get; set; } = new();
	public Dictionary<string, object>? source { get; set; }

	public string op { get; set; } = "I";

	public long ts_ms { get; set; } =  DateTime.UtcNow.Ticks;

	public void SetValue(string field, string type, object value, bool isPrimary, bool option) 
	{
		if (!payload.ContainsKey(field))
		{
			schema.fields.Add(new KafkaMessageField(type, isPrimary, option, field));
			payload.Add(field, value);
		}
	}

	public string ToJsonString() => JsonSerializer.Serialize(this);
}
