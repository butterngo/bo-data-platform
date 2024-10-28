using Bo.Kafka;
using Avro.Generic;
using Confluent.Kafka;
using Paillave.Etl.Core;

namespace BO.PG.SinkConnector.ValuesProvider;

public class KafkaSourceArgs
{
	//public required string BootstrapServers { get; set; }
	public required string Topic { get; set; }
	public required string GroupId { get; set; }
}

internal class KafkaConsumeValuesProvider : IValuesProvider<string, Message<string, GenericRecord>>
{
	public string TypeName => "Kafka Consumer";
	public ProcessImpact PerformanceImpact => ProcessImpact.Light;
	public ProcessImpact MemoryFootPrint => ProcessImpact.Light;

	public void PushValues(string input, Action<Message<string, GenericRecord>> push, CancellationToken cancellationToken, IDependencyResolver resolver, IInvoker invoker)
	{
		var consumer = resolver.Resolve<IKafkaConsumer>();

		var kafkaSourceArgs = resolver.Resolve<KafkaSourceArgs>();

		consumer.Create(options =>
		{
			options.GroupId = kafkaSourceArgs.GroupId;
		});

		consumer.Subscribe(kafkaSourceArgs.Topic, consumeResult =>
		{
			push.Invoke(consumeResult.Message);

			return Task.CompletedTask;

		}, cancellationToken);
	}
}
