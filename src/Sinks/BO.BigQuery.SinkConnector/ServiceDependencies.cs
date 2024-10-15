using BO.Core.Interfaces;
using BO.BigQuery.SinkConnector.Models;
using BO.BigQuery.SinkConnector.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace BO.Connectors;

public static class ServiceDependencies
{
	public static IServiceCollection AddBigQueryDestConnector(this IServiceCollection services)
	{
		services.AddTransient<ITaskRunHandler, TaskRunBigQueryHandler>();

		services.AddTransient<ISinkConnectorMappingHandler, SinkConnectorMappingHandler>();

		return services;
	}
}
