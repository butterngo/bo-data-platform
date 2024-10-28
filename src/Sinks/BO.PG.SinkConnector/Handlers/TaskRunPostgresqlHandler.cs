using Bo.Kafka;
using BO.Core.Entities;
using System.Text.Json;
using Paillave.Etl.Core;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using Microsoft.Extensions.Logging;
using BO.PG.SinkConnector.ValuesProvider;
using Npgsql;
using BO.PG.SinkConnector.ValuesProviders;


namespace BO.PG.SinkConnector.Handlers;

public class TaskRunPostgresqlHandler : TaskRunBaseHandler<TaskRunPostgresqlHandler>
{
	private readonly IDestinationRepository _destinationRepository;
	private readonly TableRepository _tableRepository;

	private PgAppConfiguration AppConfiguration { get; set; }

	private IKafkaConsumer Consumer { get; set; }

	public TaskRunPostgresqlHandler(IDestinationRepository destinationRepository,
		ITaskRunRepository taskRunRepository,
		ILoggerFactory loggerFactory,
		IKafkaConsumer consumer,
		TableRepository tableRepository)
		:base(taskRunRepository, loggerFactory)
	{
		Consumer = consumer;
		_tableRepository = tableRepository;
		_destinationRepository = destinationRepository;
	}

	protected override void Dispose(bool isDispose)
	{
		Consumer?.Dispose();
	}

	protected override Task OnBeforeCompleting(TaskRun state, CancellationToken cancellationToken)
	=> Task.CompletedTask;

	protected override async Task DoWork(TaskRun state, CancellationToken cancellationToken)
	{
		var destination = await _destinationRepository.GetByAsync(state.ReferenceId);

		if (destination == null)
		{
			throw new InvalidOperationException($"not found referenceId: {state.ReferenceId}");
		}

		AppConfiguration = PgAppConfiguration.Deserialize<PgAppConfiguration>(destination.AppConfiguration);

		var kafkaSourceArgs = new KafkaSourceArgs
		{
			Topic = AppConfiguration.TopicPattern,
			GroupId = destination.Name,
		};

		var processRunner = StreamProcessRunner.Create<string>(DefineProcess);
		processRunner.DebugNodeStream += (sender, e) => { /* PLACE A CONDITIONAL BREAKPOINT HERE FOR DEBUG ex: e.NodeName == "parse file" */ };
		var executionOptions = new ExecutionOptions<string>
		{
			CancellationToken = cancellationToken,
			TraceProcessDefinition = DefineTraceProcess,
			Resolver = new SimpleDependencyResolver()
							.Register(Consumer).Register(kafkaSourceArgs).Register(_tableRepository)
		};

		var res = await processRunner.ExecuteAsync("Stream data 1", executionOptions);
		
	}
	private static void DefineTraceProcess(IStream<TraceEvent> traceStream, ISingleStream<string> contentStream)
	{
		// TODO: Define the ETL process to handle traces here
	}

	private static void DefineProcess(ISingleStream<string> contextStream) 
	{
		//var stream1 = contextStream
		//.CrossApply("consumer kafka ", new KafkaConsumeValuesProvider())
		//.Select("extract message", message => 
		//{
		//	return message.Value.Schema.Fields.ToDictionary(field => field.Name, field => message.Value[field.Name]);
		//}).Do("show data", item => 
		//{
		//	Console.WriteLine(JsonSerializer.Serialize(item));
		//});

		contextStream
			   .PostgreSqlSource("select data from table", o => o
				   .FromTable("northwind.products")
				   .SelectColumns("product_id", "product_name"))
			   .Select("process data", i => i)
			   .Do("show data", item =>
			   {
				   Console.WriteLine(JsonSerializer.Serialize(item));
			   }); ;
	}
}
