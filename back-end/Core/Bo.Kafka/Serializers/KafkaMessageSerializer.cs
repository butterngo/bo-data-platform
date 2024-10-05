using Confluent.Kafka;

namespace Bo.Kafka.Serializers;

public class KafkaMessageSerializer<T> : ISerializer<T>, IDeserializer<T>
{
    public byte[] Serialize(T data, SerializationContext context) => JsonSerializerKafka.SerializeToUtf8Bytes(data);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        (isNull
            ? default
            : JsonSerializerKafka.Deserialize<T>(data.ToArray())
              ?? throw new AggregateException("Can not Deserialize message"))!;

    public static KafkaMessageSerializer<T> Create() => new();
}
