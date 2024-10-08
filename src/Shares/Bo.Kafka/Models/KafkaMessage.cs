
using System.Text.Json;

namespace Bo.Kafka.Models;

public record struct KafkaMessageSchema(string type, List<object> fields);

public class KafkaMessage
{
	public KafkaMessageSchema schema { get; set; } = new KafkaMessageSchema("struct", new List<object>());
	public Dictionary<string, object> payload { get; set; } = new();
	public Dictionary<string, object>? source { get; set; }

	public string op { get; set; } = "I";

	public long ts_ms { get; set; } =  DateTime.UtcNow.Ticks;

	public void SetValue(string field, string type, object value, bool option) 
	{
		if (!payload.ContainsKey(field))
		{
			schema.fields.Add(new { type, option, field });
			payload.Add(field, value);
		}
	}

	public string ToJsonString() => JsonSerializer.Serialize(this);
}
