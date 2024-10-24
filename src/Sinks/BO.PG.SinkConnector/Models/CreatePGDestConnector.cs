﻿using BO.Core;
using BO.Core.Models;
using BO.Core.Entities.Enums;

namespace BO.PG.SourceConnector.Models;

public class CreatePGDestConnector : ConnectorBaseModel
{
	public override string AppName => Constants.AppNames.POSTGRESQL;
	public required string Schema { get; set; }
	public string? Topics { get; set; }
	public string? TopicPattern { get; set; }
	public required string ConnectionString { get; set; }
	public JsonSchemaType SchemaType { get; set; }
	public string? KafkaServer { get; set; }

	public Dictionary<string, object> Consumer => new Dictionary<string, object>
	{
		{ "groupId", $"bo_pg_sink_connector-{Schema}".ToLower() },
		{ "kafkaServer", KafkaServer }
	};
}
