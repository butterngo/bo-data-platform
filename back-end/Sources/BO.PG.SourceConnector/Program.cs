using BO.PG.SourceConnector;
using BO.PG.SourceConnector.Abstractions;
using BO.PG.SourceConnector.Apis;
using BO.PG.SourceConnector.Entities;
using BO.PG.SourceConnector.Handlers;
using BO.PG.SourceConnector.Repositories;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
// Add services to the container.
builder.Services.AddProblemDetails();

builder.Services.AddSingleton(p => new DataContext(p.GetRequiredService<IConfiguration>().GetConnectionString("master")));

builder.Services.AddSingleton<ITaskManagement, TaskManagement>();

builder.Services.AddSingleton<ISourceRepository, SourceRepository>();

builder.Services.AddSingleton<ITaskRunRepository, TaskRunRepository>();

builder.Services.AddTransient<IPGPublicationAndSlotHandler, PGPublicationAndSlotHandler>();

builder.Services.AddScoped<ITaskRunHandler, TaskRunKafkaHandler>();

builder.Services.Configure<JsonSerializerOptions>(options =>
{
	options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
	options.IncludeFields = true;
	options.PropertyNameCaseInsensitive = true;
});

builder.Services.AddHostedService<Worker>();

var app = builder.Build();



// Configure the HTTP request pipeline.
app.MapGroup("/connectors")
	.MapConnectorsApiEndpoints()
	.WithTags("Connectors");

app.UseRouting();

app.Run();
