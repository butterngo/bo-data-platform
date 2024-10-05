namespace BO.PG.SourceConnector.Abstractions;

public interface IPGPublicationAndSlotHandler
{
	Task<(string PublicationName, string SlotName)> HandleAsync(string connectionString,
		string publicationName,
		string slotName,
		string tables, CancellationToken cancellationToken);
}
