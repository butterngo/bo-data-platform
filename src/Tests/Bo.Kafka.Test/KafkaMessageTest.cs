using Avro;
using Avro.Generic;
using System;
using static System.Net.Mime.MediaTypeNames;

namespace Bo.Kafka.Test
{
	public class KafkaMessageTest
	{

		[Fact]
		public void Test()
		{
            var json = @"{
    ""type"": ""record"",
    ""name"": ""categories"",
    ""namespace"": ""northwind"",
    ""fields"": [
        {
            ""name"": ""category_id"",
            ""type"": ""int""
        },
        {
            ""name"": ""category_name"",
            ""type"": ""string""
        },
         { ""name"": ""description"", ""type"": ""int"" },
        {
            ""name"": ""picture"",
            ""type"": ""null""
        }
    ]
}";
			var avroSchema = (RecordSchema)RecordSchema.Parse(json);

			var record = new GenericRecord(avroSchema);
			record.Add("category_id", 1);
			record.Add("category_name", "test");
			record.Add("description", "blue");
			record.Add("picture", 1);
			
			var aa = "";
		}

	}

}