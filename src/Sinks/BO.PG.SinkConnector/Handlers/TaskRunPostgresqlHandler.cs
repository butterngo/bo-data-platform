using Bo.Kafka;
using Avro.Generic;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.Core.Implementations;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using BO.Core.Converters;
using NpgsqlTypes;
using Avro;
using Avro.Util;

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

	private async Task ExecuteAsync(GenericRecord kafkaMessage, CancellationToken cancellationToken)
	{
		//cancellationToken.ThrowIfCancellationRequested();

		//var tableName = _tableRepository.ConvertTableName(kafkaMessage, AppConfiguration.Schema);

		//switch (kafkaMessage.op.ToUpper()) 
		//{
		//	case "I": 
		//		{
		//			await _tableRepository.InsertAsync(AppConfiguration.ConnectionString, tableName, kafkaMessage, cancellationToken);
		//			break;
		//		}
		//	case "U":
		//		{
		//			await _tableRepository.UpdateAsync(AppConfiguration.ConnectionString, tableName, kafkaMessage, cancellationToken);
		//			break;
		//		}
		//	case "D":
		//		{
		//			throw new NotImplementedException();
		//		}
		//}
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

		//await _tableRepository.CreateSchemaIfNotExited(AppConfiguration.ConnectionString, AppConfiguration.Schema, cancellationToken);

		Consumer.Create(options => 
		{
			options.GroupId = AppConfiguration.Consumer["groupId"].ToString();
		});

		if (AppConfiguration.Topics != null)
		{
			await Consumer.Subscribe(AppConfiguration.Topics, async consumeResult =>
			{
				Console.WriteLine($"Key: {consumeResult.Message.Key}\nValue: {consumeResult.Message.Value}");

				//await ExecuteAsync(message, cancellationToken);

			}, cancellationToken);

		}

		if (!string.IsNullOrEmpty(AppConfiguration.TopicPattern))
		{
			await Consumer.Subscribe(AppConfiguration.TopicPattern, async consumeResult =>
			{
				try
				{
					var fields = consumeResult.Message.Value.Schema.Fields;
					Console.WriteLine($"table {GenerateTable(consumeResult.Message.Value.Schema)}");
					foreach (var field in fields)
					{
						Console.WriteLine($"{field.Name}: {consumeResult.Message.Value[field.Name]}");
					}
				}
				catch (Exception ex) 
				{
				}
				

				//await ExecuteAsync(message, cancellationToken);

			}, cancellationToken);
		}
	}

	private static string GetLogicalType(Schema schema) 
	{
		string logicalType = null;
		if (schema is LogicalSchema logicalSchema)
		{
			logicalType = logicalSchema.LogicalTypeName;
		}
		else if (schema is UnionSchema unionSchema)
		{
			foreach (var subSchema in unionSchema.Schemas)
			{
				if (subSchema is LogicalSchema logicalSubSchema)
				{
					logicalType = logicalSubSchema.LogicalTypeName;
					break;
				}
			}
		}

		return logicalType;
	}

	private static string GetAvroType(Schema schema) 
	{
		bool isUnionSchema = schema is UnionSchema;

		string type = schema.Name;

		if (isUnionSchema)
		{
			type = (schema as UnionSchema).Schemas.Last().Name;
		}
	
		return type;
	}

	private static string GenerateTable(RecordSchema avroSchema)
	{
		string tableName = avroSchema.Name;
		
		List<string> columns = new List<string>();

		foreach (var field in avroSchema.Fields)
		{
			string fieldName = field.Name;
			string avroType = GetAvroType(field.Schema);
			string logicalType = GetLogicalType(field.Schema);

			NpgsqlDbType postgresType = TypeConverterHelper.ConvertAvroTypeToNpgsqlDbType(avroType, logicalType);

			columns.Add($"{fieldName} {postgresType.ToString()}");
		}

		string createTableSQL = $"CREATE TABLE {tableName} (\n  {string.Join(",\n  ", columns)}\n);";

		Console.WriteLine(createTableSQL);

		return tableName;
	}
}
