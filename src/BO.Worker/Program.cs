using BO.Core.Interfaces;
using BO.Worker;
using BO.Core;
using BO.Core.Models;
using BO.Worker.Handlers;
using BO.PG.SourceConnector.Handlers;

var builder = WebApplication.CreateBuilder(args);

Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;

builder.Services.AddCore(options =>
{
	builder.Configuration.GetSection(nameof(TaskManagementOptions))
	.Bind(options);
});

builder.Services.AddSingleton<ITaskRunFactory, TaskRunFactory>();

builder.Services.AddTransient<ITaskRunHandler, TaskRunBigQueryHandler>();

builder.Services.AddTransient<ITaskRunHandler, TaskRunKafkaHandler>();

builder.Services.AddHostedService<SinkConnectorWorker>();

var app = builder.Build();

app.Run();
