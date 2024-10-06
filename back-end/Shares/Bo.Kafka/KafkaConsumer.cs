using Bo.Kafka.Models;
using Bo.Kafka.Serializers;
using Confluent.Kafka;

namespace Bo.Kafka;

public class KafkaConsumer : IDisposable
{
	private readonly IConsumer<Ignore, KafkaMessage> _consumer;

	public KafkaConsumer(ConsumerConfig config) 
	{
		var consumerBuilder = new ConsumerBuilder<Ignore, KafkaMessage>(config);
		consumerBuilder.SetValueDeserializer(KafkaMessageSerializer<KafkaMessage>.Create());
		consumerBuilder.SetLogHandler((_, logHandler) => { });
		consumerBuilder.SetErrorHandler((_, errorHandler) => { });
		_consumer = consumerBuilder.Build();
	}

	public void Consume(IEnumerable<string> topics, Func<KafkaMessage> func, CancellationToken cancellationToken) 
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			_consumer.Subscribe(topics);

			var consumeResult = _consumer.Consume(cancellationToken);

			_consumer.Commit(consumeResult);
		}
	}

	public void Dispose()
	{
		_consumer.Close();
	}
}
