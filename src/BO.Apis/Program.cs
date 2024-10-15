using BO.Apis;
using BO.Core;
using BO.Connectors;
using BO.Core.Models;

var builder = WebApplication.CreateBuilder(args);

Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
// Add services to the container.
builder.Services.AddProblemDetails();

builder.Services.AddCore(options =>
{
	builder.Configuration.GetSection(nameof(TaskManagementOptions))
	.Bind(options);
});

builder.Services.AddPostgresqlSrcConnector();

builder.Services.AddPostgresqlDestConnector();

builder.Services.AddBigQueryDestConnector();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGroup("/connectors")
	.MapConnectorsApiEndpoints()
	.WithTags("Connectors");

app.UseRouting();

app.Run();