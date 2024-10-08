using System.Data;

namespace BO.Core.Interfaces;

public interface IDataContext
{
	IDbConnection CreateConnection();
}
