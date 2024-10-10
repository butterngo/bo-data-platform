using BO.Core.Entities.Enums;

namespace BO.Core.Interfaces;

public interface ITaskRunFactory
{
	ITaskRunHandler GetHander(string name, TaskRunType taskRunType);
}
