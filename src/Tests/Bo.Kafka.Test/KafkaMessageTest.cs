using Avro;
using Avro.Generic;

namespace Bo.Kafka.Test
{
	public class KafkaMessageTest
	{

		[Fact]
		public void Test()
		{
            var json = @"{
  ""type"": ""record"",
  ""name"": ""User"",
  ""fields"": [
    {""name"": ""birth_date"", ""type"": {""type"": ""int"", ""logicalType"": ""date""}}
  ]
}";
			
			var avroSchema = (RecordSchema)RecordSchema.Parse(json);

			var field = avroSchema.Fields.First().Schema;
			var record = new GenericRecord(avroSchema);
	
		}

	}

}