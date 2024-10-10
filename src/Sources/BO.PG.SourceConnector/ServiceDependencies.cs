using BO.Core.Interfaces;
using BO.PG.SourceConnector.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace BO.Connectors;

public static class ServiceDependencies
{
	public static IServiceCollection AddPostgresqlSrcConnector(this IServiceCollection services) 
	{
		services.AddTransient<ITaskRunHandler, TaskRunPostgresqlHandler>();

		return services;
	}
}
