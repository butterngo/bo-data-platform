using BO.Core.Models;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.Core.Implementations;

internal class DestinationConnectorService : IDestinationConnectorService
{
	private readonly ITaskRunRepository _taskRunRepository;
	private readonly IDestinationRepository _destinationRepository;
	private readonly ILogger<DestinationConnectorService> _logger;
	private readonly IEnumerable<ISinkConnectorMappingHandler> _sinkConnectorMappingHandlers;

	public DestinationConnectorService(IDestinationRepository destinationRepository,
		ITaskRunRepository taskRunRepository,
		ILogger<DestinationConnectorService> logger,
		IEnumerable<ISinkConnectorMappingHandler> sinkConnectorMappingHandler) 
	{
		_logger = logger;
		_taskRunRepository = taskRunRepository;
		_destinationRepository = destinationRepository;
		_sinkConnectorMappingHandlers = sinkConnectorMappingHandler;
	}

	private SinkConnectorMappingBaseHandler<TInput> GetHandler<TInput>() where TInput : ConnectorBaseModel
	{
		var sinkConnectorMappingHandler = _sinkConnectorMappingHandlers
		.FirstOrDefault(x => x.GetType().BaseType
		.Equals(typeof(SinkConnectorMappingBaseHandler<TInput>)));

		if(sinkConnectorMappingHandler == null)
		{
			throw new InvalidOperationException($"Not found ISinkConnectorMappingHandler : {typeof(SinkConnectorMappingBaseHandler<TInput>).Name}");
		}

		return sinkConnectorMappingHandler as SinkConnectorMappingBaseHandler<TInput>;
	}

	public async Task<Destination> CreateAsync<TInput>(TInput input, CancellationToken cancellationToken) where TInput : ConnectorBaseModel
	{
		var sinkConnectorMappingHandler = GetHandler<TInput>();

		var (destination, taskRun) = await sinkConnectorMappingHandler.HandlAsync(input, cancellationToken);

		await _destinationRepository.CreateAsync(destination);

		await _taskRunRepository.AddTaskAsync(taskRun, cancellationToken);

		return destination;
	}
}
