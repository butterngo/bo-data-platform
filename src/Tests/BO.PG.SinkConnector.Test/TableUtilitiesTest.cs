using BO.PG.SinkConnector.StreamProviders;
using Confluent.Kafka;
using Paillave.Etl.Core;

namespace BO.PG.SinkConnector.Test;

public class TableUtilitiesTest
{
	[Fact]
	public void Test1()
	{
		var processRunner = StreamProcessRunner.Create<string>(DefineProcess);
		processRunner.DebugNodeStream += (sender, e) => { /* PLACE A CONDITIONAL BREAKPOINT HERE FOR DEBUG ex: e.NodeName == "parse file" */ };
	}

	private static void DefineTraceProcess(IStream<TraceEvent> traceStream, ISingleStream<string> contentStream)
	{
		// TODO: Define the ETL process to handle traces here
	}
	private static void DefineProcess(ISingleStream<string> contextStream)
	{
		//var stream1 = contextStream
		// .CrossApplyKafka("consume kafka", new KafkaSourceArgs
		// {
		//	 BootstrapServers = "your_kafka_bootstrap_servers",
		//	 Topic = "your_kafka_topic",
		//	 GroupId = "your_group_id",
		//	 KeyDeserializer = Deserializers.Utf8,
		//	 ValueDeserializer = Deserializers.Utf8
		// });
	}
}