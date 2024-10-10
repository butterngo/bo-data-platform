using BO.Core.Interfaces;
using BO.PG.SourceConnector.Models;

namespace BO.Apis;

public static class ConnectorsApi
{
	public static RouteGroupBuilder MapConnectorsApiEndpoints(this RouteGroupBuilder groups)
	{
		//groups.MapGet("/", GetAllTodoItems).Produces(200, typeof(PagedResults<TodoItemOutput>)).ProducesProblem(401).Produces(429);
		//groups.MapGet("/{id}", GetTodoItemById).Produces(200, typeof(TodoItemOutput)).ProducesProblem(401).Produces(429);
		groups.MapPost("/sources/postgresql", CreateSrcConnector);
		groups.MapPost("/destinations/postgresql", CreateDestConnector);
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

	internal static async Task<IResult> CreateDestConnector(IServiceProvider provider,
		CreatePGDestConnector input,
		CancellationToken cancellationToken)
	{
		var sinkConnectorMappingHandler = provider.GetRequiredService<ISinkConnectorMappingHandler<CreatePGDestConnector>>();

		var destinationRepository = provider.GetRequiredService<IDestinationRepository>();

		var taskRunRepository = provider.GetRequiredService<ITaskRunRepository>();

		var (destination, taskRun) = await sinkConnectorMappingHandler.HandlAsync(input);

		await destinationRepository.CreateAsync(destination);

		await taskRunRepository.AddTaskAsync(taskRun, cancellationToken);

		return TypedResults.Ok(destination);
	}
}
