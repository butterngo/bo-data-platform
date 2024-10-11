namespace BO.Core.Models;

public class CreateSrcConnectorModel : Dictionary<string, object>
{
	public required string AppName { get; set; }
}
