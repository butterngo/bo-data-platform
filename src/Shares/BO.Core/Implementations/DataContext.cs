using Npgsql;
using System.Data;
using BO.Core.Interfaces;

namespace BO.Core.Implementations;

public class DataContext : IDataContext
{
	private readonly string _connectionString;

	public DataContext(string connectionString)
	{
		_connectionString = connectionString;
	}

	public IDbConnection CreateConnection()
	{
		return new NpgsqlConnection(_connectionString);
	}
}
