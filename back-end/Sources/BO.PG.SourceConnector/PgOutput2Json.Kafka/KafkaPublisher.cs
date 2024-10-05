namespace PgOutput2Json.Kafka;

public class KafkaPublisher : IMessagePublisher
{
	public void Dispose()
	{
		//
	}

	public void ForceConfirm()
	{
		//
	}

	public bool Publish(string json, string tableName, string keyColumnValue, int partition)
	{
		throw new NotImplementedException();
	}
}