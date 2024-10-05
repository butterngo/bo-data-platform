using System.ComponentModel;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace Bo.Kafka.Serializers;

public class JsonSerializerKafka
{
    public static string Serialize<T>(T data) => JsonSerializer.Serialize(data, GetJsonSerializerOptions());

    public static T? Deserialize<T>(ReadOnlySpan<byte> data) =>
        JsonSerializer.Deserialize<T>(data.ToArray(), GetJsonSerializerOptions()) ?? default;

    public static byte[] SerializeToUtf8Bytes<T>(T data) =>
        JsonSerializer.SerializeToUtf8Bytes(data, GetJsonSerializerOptions());

    private static JsonSerializerOptions GetJsonSerializerOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = false,
            WriteIndented = false,
            AllowTrailingCommas = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            ReferenceHandler = ReferenceHandler.IgnoreCycles,
            Converters = { new DateTimeConverter() },
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip
        };
    }
}
