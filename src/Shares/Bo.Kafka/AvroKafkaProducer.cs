﻿using Avro.Generic;
using Bo.Kafka.Models;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Bo.Kafka;

public interface IKafkaProducer : IDisposable
{
	Task ProduceAsync(string topic, string key, GenericRecord message, CancellationToken cancellationToken = default(CancellationToken));
}

internal class AvroKafkaProducer : IKafkaProducer
{
	private readonly IProducer<string, GenericRecord> _producer;

	public Func<ProduceException<string, GenericRecord>, Task> OnError { get; set; }

	public AvroKafkaProducer(KafkaOptions kafkaOptions, ISchemaRegistryClient schemaRegistryClient)
	{
		var avroSerializerConfig = new AvroSerializerConfig();

		_producer = new ProducerBuilder<string, GenericRecord>(kafkaOptions.ProducerConfig)
			.SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistryClient, avroSerializerConfig))
			.Build();
	}

	public async Task ProduceAsync(string topic, string key, GenericRecord message, CancellationToken cancellationToken = default(CancellationToken))
	{
		await _producer.ProduceAsync(topic, new Message<string, GenericRecord>
		{
			Key = key,
			Value = message
		}, cancellationToken);
	}

	public void Dispose()
	{
		_producer.Dispose();
	}
}
