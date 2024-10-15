using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.PG.SourceConnector.Models;
using BO.BigQuery.SinkConnector.Models;

namespace BO.Apis;

public static class ConnectorsApi
{
	public static RouteGroupBuilder MapConnectorsApiEndpoints(this RouteGroupBuilder groups)
	{
		//groups.MapGet("/", GetAllTodoItems).Produces(200, typeof(PagedResults<TodoItemOutput>)).ProducesProblem(401).Produces(429);
		//groups.MapGet("/{id}", GetTodoItemById).Produces(200, typeof(TodoItemOutput)).ProducesProblem(401).Produces(429);
		groups.MapPost("/sources/postgresql", CreateSrcConnector);
		groups.MapPost("/destinations/postgresql", CreatePgDestConnector);
		groups.MapPost("/destinations/bigquery", CreateBigQueryDestConnector);
		//groups.MapPut("/{id}", UpdateTodoItem).Accepts<TodoItemInput>("application/json").Produces(201).ProducesProblem(404).ProducesProblem(401).Produces(429);
		//groups.MapDelete("/{id}", DeleteTodoItem).Produces(204).ProducesProblem(404).ProducesProblem(401).Produces(429);
		return groups;
	}

	internal static async Task<IResult> CreateSrcConnector(IServiceProvider provider,
		CreatePGSrcConnector input,
		CancellationToken cancellationToken)
	{
		var srcConnectorMappingHandler = provider.GetRequiredService<ISrcConnectorMappingHandler<CreatePGSrcConnector>>();

		var soureRepository = provider.GetRequiredService<ISourceRepository>();

		var taskRunRepository = provider.GetRequiredService<ITaskRunRepository>();

		var (source, taskRuns) = await srcConnectorMappingHandler.HandlAsync(input);

		await soureRepository.CreateAsync(source);

		await taskRunRepository.AddTaskAsync(taskRuns, cancellationToken);

		return TypedResults.Ok(source);
	}

	internal static async Task<IResult> CreatePgDestConnector(IDestinationConnectorService destinationConnectorService,
		CreatePGDestConnector input,
		CancellationToken cancellationToken)
	{
		return TypedResults.Ok(await destinationConnectorService.CreateAsync(input, cancellationToken));
	}

	internal static async Task<IResult> CreateBigQueryDestConnector(IDestinationConnectorService destinationConnectorService,
		CreateBigQueryDestConnector input,
		CancellationToken cancellationToken)
	{
		return TypedResults.Ok(await destinationConnectorService.CreateAsync(input, cancellationToken));
	}
}
