using Npgsql;
using System.Data;

namespace BO.PG.SourceConnector.Entities;
public class DataContext
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

	//public async Task Init()
	//{
	//	await _initDatabase();
	//	await _initTables();
	//}

	//private async Task _initDatabase()
	//{
	//	// create database if it doesn't exist
	//	var connectionString = $"Host={_dbSettings.Server}; Database=postgres; Username={_dbSettings.UserId}; Password={_dbSettings.Password};";
	//	using var connection = new NpgsqlConnection(connectionString);
	//	var sqlDbCount = $"SELECT COUNT(*) FROM pg_database WHERE datname = '{_dbSettings.Database}';";
	//	var dbCount = await connection.ExecuteScalarAsync<int>(sqlDbCount);
	//	if (dbCount == 0)
	//	{
	//		var sql = $"CREATE DATABASE \"{_dbSettings.Database}\"";
	//		await connection.ExecuteAsync(sql);
	//	}
	//}

	//private async Task _initTables()
	//{
	//	// create tables if they don't exist
	//	using var connection = CreateConnection();
	//	await _initUsers();

	//	async Task _initUsers()
	//	{
	//		var sql = """
 //               CREATE TABLE IF NOT EXISTS Users (
 //                   Id SERIAL PRIMARY KEY,
 //                   Title VARCHAR,
 //                   FirstName VARCHAR,
 //                   LastName VARCHAR,
 //                   Email VARCHAR,
 //                   Role INTEGER,
 //                   PasswordHash VARCHAR
 //               );
 //           """;
	//		await connection.ExecuteAsync(sql);
	//	}
	//}
}
