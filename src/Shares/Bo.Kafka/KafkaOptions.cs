using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Bo.Kafka;

public class KafkaOptions
{
    public string SchemaRegistryUrl { get; set; } = "http://localhost:8081";
    public string BootstrapServers { get; set; } = "localhost:29092";
    public string ConsumerGroupId { get; set; } = "bo_data_platform_developer";

	private ConsumerConfig? _consumerConfig;

    public ConsumerConfig ConsumerConfig
    {
        get
        {
            if (_consumerConfig == null)
            {
                _consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = BootstrapServers,
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
					EnableAutoCommit = false,
                    GroupId = ConsumerGroupId,
				};
            }

            return _consumerConfig;
        }
    }

    private ProducerConfig? _producerConfig;

    public ProducerConfig ProducerConfig
    {
        get
        {
            if (_producerConfig == null)
            {
                _producerConfig = new ProducerConfig
                {
                    BootstrapServers = BootstrapServers
                };
            }

            return _producerConfig;
        }
    }

    private SchemaRegistryConfig? _schemaRegistryConfig;

    public SchemaRegistryConfig SchemaRegistryConfig
    {
        get
        {
            if (_schemaRegistryConfig == null)
            {
                _schemaRegistryConfig = new SchemaRegistryConfig
                {
                    Url = SchemaRegistryUrl
                };
            }

            return _schemaRegistryConfig;
        }
    }
}

