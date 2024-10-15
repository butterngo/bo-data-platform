using BO.Core.Interfaces;
using BO.PG.SinkConnector.Handlers;
using BO.PG.SourceConnector.Models;
using Microsoft.Extensions.DependencyInjection;

namespace BO.Connectors;

public static class ServiceDependencies
{
	public static IServiceCollection AddPostgresqlDestConnector(this IServiceCollection services)
	{
		services.AddTransient<ITaskRunHandler, TaskRunPostgresqlHandler>();

		services.AddTransient<ISinkConnectorMappingHandler, SinkConnectorMappingHandler>();

		return services;
	}
}
