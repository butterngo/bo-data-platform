using BO.Core.Interfaces;
using BO.PG.SinkConnector.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace BO.Connectors;

public static class ServiceDependencies
{
	public static IServiceCollection AddPostgresqlDestConnector(this IServiceCollection services)
	{
		services.AddTransient<ITaskRunHandler, TaskRunPostgresqlHandler>();

		return services;
	}
}
