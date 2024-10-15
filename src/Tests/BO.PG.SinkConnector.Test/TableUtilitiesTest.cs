using Bo.Kafka.Models;
using System.Text.Json;

namespace BO.PG.SinkConnector.Test;

public class TableUtilitiesTest
{
	[Fact]
	public void Test1()
	{
		var json = File.ReadAllText(Path.Combine("TestData", "kafka_message.json"));

		var kafkaMessage = JsonSerializer.Deserialize<KafkaMessage>(json);

		//var sciprt = TableUtilities.GenerateInsertScript(kafkaMessage.source["table"].ToString(), kafkaMessage.payload);

		//var script = TableUtilities.GenerateCreateTableScript(kafkaMessage.schema.fields, kafkaMessage.source["table"].ToString());
		//KafkaMessage kafkaMessage
	}
}