using System.Text.Json;
using System.Text.Json.Serialization;

namespace BO.Core.Models;

public abstract class AppConfiguration
{
	[JsonPropertyName("publisher")]
	public Dictionary<string, object> Publisher { get; set; } = new();

	[JsonPropertyName("consumer")]
	public Dictionary<string, object> Consumer { get; set; } = new();

	public abstract string Serialize();

    public static T Deserialize<T>(string json) where T : AppConfiguration
	{
        return JsonSerializer.Deserialize<T>(json, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
    }
}
