namespace BO.Core.Models;

public abstract class ConnectorBaseModel 
{
	public abstract string AppName { get; }
	public required string Name { get; set; }
}
