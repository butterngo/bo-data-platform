using BO.Apis;
using BO.Core;
using BO.Core.Interfaces;
using BO.Core.Models;
using BO.PG.SourceConnector.Handlers;
using BO.PG.SourceConnector.Models;

var builder = WebApplication.CreateBuilder(args);

Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
// Add services to the container.
builder.Services.AddProblemDetails();

builder.Services.AddCore(options =>
{
	builder.Configuration.GetSection(nameof(TaskManagementOptions))
	.Bind(options);
});

builder.Services.AddSingleton<ISrcConnectorMappingHandler<CreatePGSrcConnector>, SrcConnectorMappingHandler>();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGroup("/connectors")
	.MapConnectorsApiEndpoints()
	.WithTags("Connectors");

app.UseRouting();

app.Run();