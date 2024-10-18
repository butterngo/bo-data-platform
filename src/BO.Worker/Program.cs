using BO.Core;
using BO.Worker;
using Bo.Kafka;
using BO.Connectors;
using BO.Core.Models;
using BO.Core.Interfaces;
//https://aloneguid.github.io/parquet-dotnet/starter-topic.html#quick-start
var builder = WebApplication.CreateBuilder(args);

Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;

builder.Services.AddCore(options =>
{
	builder.Configuration.GetSection(nameof(TaskManagementOptions))
	.Bind(options);
});

builder.Services.AddKafka(options =>
{
	builder.Configuration.GetSection("Kafka")
	.Bind(options);
});

builder.Services.AddScoped<ITaskRunFactory, TaskRunFactory>();


builder.Services.AddPostgresqlSrcConnector();

//builder.Services.AddBigQueryDestConnector();

//builder.Services.AddPostgresqlDestConnector();

builder.Services.AddHostedService<WorkerService>();

var app = builder.Build();

app.Run();
