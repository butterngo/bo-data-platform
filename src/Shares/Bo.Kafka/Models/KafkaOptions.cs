using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Bo.Kafka.Models;

public class KafkaOptions 
{
    public ProducerConfig? ProducerConfig { get; set; }
	public SchemaRegistryConfig? SchemaRegistryConfig { get; set; }
}

