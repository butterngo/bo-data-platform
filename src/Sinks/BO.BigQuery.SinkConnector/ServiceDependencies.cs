using BO.BigQuery.SinkConnector.Handlers;
using BO.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace BO.Connectors;

public static class ServiceDependencies
{
	public static IServiceCollection AddBigQueryDestConnector(this IServiceCollection services)
	{
		services.AddTransient<ITaskRunHandler, TaskRunBigQueryHandler>();

		return services;
	}
}
