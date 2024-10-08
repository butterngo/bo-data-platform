namespace BO.Core.Interfaces;

public interface ITaskRunFactory
{
	ITaskRunHandler GetHander(string name);
}
