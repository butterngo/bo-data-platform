using BO.PG.SourceConnector.Abstractions;
using BO.PG.SourceConnector.Dtos;
using BO.PG.SourceConnector.Entities;
using BO.PG.SourceConnector.Extensions;
using BO.PG.SourceConnector.Models;
using BO.PG.SourceConnector.Repositories;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace BO.PG.SourceConnector.Apis;

public static class ConnectorsApi
{
	public static RouteGroupBuilder MapConnectorsApiEndpoints(this RouteGroupBuilder groups)
	{
		//groups.MapGet("/", GetAllTodoItems).Produces(200, typeof(PagedResults<TodoItemOutput>)).ProducesProblem(401).Produces(429);
		//groups.MapGet("/{id}", GetTodoItemById).Produces(200, typeof(TodoItemOutput)).ProducesProblem(401).Produces(429);
		groups.MapPost("/", CreateConnector);
		groups.MapPost("/task", CreateTask);
		groups.MapPut("/cancel/{taskId}", CancelTask);
		//groups.MapPut("/{id}", UpdateTodoItem).Accepts<TodoItemInput>("application/json").Produces(201).ProducesProblem(404).ProducesProblem(401).Produces(429);
		//groups.MapDelete("/{id}", DeleteTodoItem).Produces(204).ProducesProblem(404).ProducesProblem(401).Produces(429);
		return groups;
	}

	internal static async Task CancelTask(ITaskManagement taskManagement, string taskId) 
	{
		taskManagement.Cancel(taskId);

	}

	internal static async Task CreateTask(ITaskManagement taskManagement) 
	{
		//for (int i = 0; i < 15; i++) 
		//{
		//	taskManagement.DoWork(i.ToString(), async (provider, token) =>
		//	{
		//		var taskId = System.Guid.NewGuid().ToString();

		//		Console.WriteLine($"Start task {taskId}");

		//		await Task.Delay(10000);

		//		Console.WriteLine($"Completed task {taskId}");
		//	});

		//}
	}

	internal static async Task<IResult> CreateConnector(ISourceRepository sourceRepository,
		ITaskRunRepository taskRunRepository,
		IPGPublicationAndSlotHandler pgPublicationAndSlotHandler,
		CreateConnectorDto dto,
		CancellationToken cancellationToken)
	{
		var dataContext = new DataContext(dto.ConnectionString);

		using var conn = dataContext.CreateConnection();

		var tables = new List<PgTableSchema>();

		var hashKey = $"{dto.Schema}_{dto.Tables}".ToSha256Hash(10);

		var source = new Source
		{
			ConnectionString = dto.ConnectionString,
			JsonPublisher = dto.JsonPublisher,
			PgPublicationName = $"BO_PG_SourceConnector_pub_{hashKey}",
			PgSlotName = $"BO_PG_SourceConnector_slot_{hashKey}_{DateTime.Now.Ticks}",
		};

		var taskRuns = new List<TaskRun>
		{
			new TaskRun
			{
				Id = source.Id,
				ReferenceId = source.Id,
				Name = $"cdc data {dto.Tables}",
				IsCdcData = true
			}
		};

		foreach (var table in dto.Tables.Split(","))
		{
			var columns = await conn.QueryAsync<string>(@"select row_to_json(table_schema) 
			from ( 
			   select column_name, data_type 
			 from INFORMATION_SCHEMA.COLUMNS where table_schema = @table_schema and table_name = @table_name 
			) table_schema;
			", new { table_schema = dto.Schema, table_name = table });

			var pgTable = new PgTableSchema(dto.Schema, table, dto.ConnectionString, JsonSerializer.Serialize(columns));

			tables.Add(pgTable);

			taskRuns.Add(new TaskRun
			{
				Id = pgTable.Id,
				Name = $"First load: {table}",
				ReferenceId = source.Id,
			});

		}

		source.JsonTable = JsonSerializer.Serialize(tables, new JsonSerializerOptions
		{
			PropertyNamingPolicy = JsonNamingPolicy.CamelCase
		});

		await sourceRepository.CreateAsync(source);

		await taskRunRepository.AddTaskAsync(taskRuns, cancellationToken);

		return TypedResults.Ok(source);
	}
}
