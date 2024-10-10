using BO.Core;
using BO.Worker;
using BO.Connectors;
using BO.Core.Models;
using BO.Core.Interfaces;

var builder = WebApplication.CreateBuilder(args);

Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;

builder.Services.AddCore(options =>
{
	builder.Configuration.GetSection(nameof(TaskManagementOptions))
	.Bind(options);
});

builder.Services.AddScoped<ITaskRunFactory, TaskRunFactory>();

builder.Services.AddBigQueryDestConnector();
builder.Services.AddPostgresqlSrcConnector();
builder.Services.AddPostgresqlDestConnector();

builder.Services.AddHostedService<WorkerService>();

var app = builder.Build();

app.Run();
