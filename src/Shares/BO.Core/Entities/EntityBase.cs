namespace BO.Core.Entities;

public abstract class EntityBase
{
	public string Id { get; set; } = Guid.NewGuid().ToString();

	public string RowVersion { get; set; } = string.Empty;
}
