using BO.Core.Models;
using BO.Core.Interfaces;
using BO.Core.Repositories;
using BO.Core.Implementations;
using BO.PG.SourceConnector.Repositories;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace BO.Core;

public static class ServiceDependencies
{
	public static IServiceCollection AddCore(this IServiceCollection services, Action<TaskManagementOptions> action)
	{
		services.AddSingleton<IDataContext>(p => new DataContext(p.GetRequiredService<IConfiguration>().GetConnectionString("master")));

		services.AddSingleton<ITaskManagement>(p => 
		{
			TaskManagementOptions options = new();

			action(options);

			return new TaskManagement(p, options);
		});

		services.AddSingleton<ISourceRepository, SourceRepository>();

		services.AddSingleton<ITaskRunRepository, TaskRunRepository>();

		services.AddSingleton<IDestinationRepository, DestinationRepository>();

		services.AddSingleton<IDestinationRepository, DestinationRepository>();

		services.AddScoped<IDestinationConnectorService, DestinationConnectorService>();

		services.AddScoped<ISourceConnectorService, SourceConnectorService>();

		return services;
	}
}
