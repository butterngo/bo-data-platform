using BO.Core.Models;
using BO.Core.Entities;
using BO.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace BO.Core.Implementations;

internal class SourceConnectorService : ISourceConnectorService
{
	private readonly ITaskRunRepository _taskRunRepository;
	private readonly ISourceRepository _sourceRepository;
	private readonly ILogger<SourceConnectorService> _logger;
	private readonly IEnumerable<ISrcConnectorMappingHandler> _srcConnectorMappingHandlers;

	public SourceConnectorService(ISourceRepository sourceRepository,
		ITaskRunRepository taskRunRepository,
		ILogger<SourceConnectorService> logger,
		IEnumerable<ISrcConnectorMappingHandler> srcConnectorMappingHandlers) 
	{
		_logger = logger;
		_sourceRepository = sourceRepository;
		_taskRunRepository = taskRunRepository;
		_srcConnectorMappingHandlers = srcConnectorMappingHandlers;
	}

	private SourceConnectorMappingBaseHandler<TInput> GetHandler<TInput>() where TInput : ConnectorBaseModel
	{
		var srcConnectorMappingHandler = _srcConnectorMappingHandlers
		.FirstOrDefault(x => x.GetType().BaseType
		.Equals(typeof(SourceConnectorMappingBaseHandler<TInput>)));

		if (srcConnectorMappingHandler == null)
		{
			throw new InvalidOperationException($"Not found SrcConnectorMappingBaseHandler : {typeof(SourceConnectorMappingBaseHandler<TInput>).Name}");
		}

		return srcConnectorMappingHandler as SourceConnectorMappingBaseHandler<TInput>;
	}

	public async Task<Source> CreateAsync<TInput>(TInput input, CancellationToken cancellationToken) where TInput : ConnectorBaseModel
	{
		var srcConnectorMappingHandler = GetHandler<TInput>();

		var (source, taskRuns) = await srcConnectorMappingHandler.HandlAsync(input, cancellationToken);

		await _sourceRepository.CreateAsync(source);

		await _taskRunRepository.AddTaskAsync(taskRuns, cancellationToken);

		return source;
	}
}
