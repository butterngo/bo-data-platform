using Bo.Kafka.Models;
using Bo.Kafka.Serializers;
using Confluent.Kafka;

namespace Bo.Kafka;

public class KafkaProducer : IDisposable
{
	private readonly IProducer<Null, KafkaMessage> _producer;

	public KafkaProducer(ProducerConfig config)
	{
		_producer = new ProducerBuilder<Null, KafkaMessage>(config)
			.SetValueSerializer(KafkaMessageSerializer<KafkaMessage>.Create())
			.Build();
	}

	public Task<DeliveryResult<Null, KafkaMessage>> ProduceAsync(string topic, KafkaMessage message, CancellationToken cancellationToken = default(CancellationToken)) 
	=> _producer.ProduceAsync(topic, new Message<Null, KafkaMessage> { Value = message }, cancellationToken);

	public void Dispose()
	{
		_producer.Dispose();
	}
}

