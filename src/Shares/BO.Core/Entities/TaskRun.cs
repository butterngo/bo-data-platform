using BO.Core.Entities.Enums;
using BO.Core.Extensions;
using BO.Core.Interfaces;

namespace BO.Core.Entities;

public class TaskRun : EntityBase, ITaskRun
{
	public required string Name { get; set; }
	public required string AppName { get; set; }
	public required string ReferenceId { get; set; }
	public TaskRunStatus Status { get; set; }
	public TaskRunType Type { get; set; }
	public bool IsCdcData { get; set; }
	public DateTime CreatedAt { get; set; }
	public DateTime? RunnedAt { get; set; }
	public DateTime? StoppedAt { get; set; }
	public DateTime? CompletedAt { get; set; }
	public DateTime? OccurredAt { get; set; }
	public string? ErrorMessage { get; set; }
	public static string Insert = @"INSERT INTO bo_connectors.task_runs
(id, ""name"", app_name, reference_id, status, ""type"", is_cdc_data, created_at, row_version)
VALUES(@id, @name, @AppName, @referenceId, @status, @type, @isCdcData, NOW(), gen_random_uuid());";
}


public static class TaskRunSchema 
{
	public static string Table => $"{nameof(TaskRun)}s".ToSnakeCase();

	public static class Columns
	{
		public static string Id { get; } = nameof(TaskRun.Id).ToSnakeCase();
		public static string Name { get; } = nameof(TaskRun.Name).ToSnakeCase();
		public static string AppName { get; } = nameof(TaskRun.AppName).ToSnakeCase();
		public static string ReferenceId { get; } = nameof(TaskRun.ReferenceId).ToSnakeCase();
		public static string Status { get; } = nameof(TaskRun.Status).ToSnakeCase();
		public static string Type { get; } = nameof(TaskRun.Type).ToSnakeCase();
		public static string IsCdcData { get; } = nameof(TaskRun.IsCdcData).ToSnakeCase();
		public static string CreatedAt { get; } = nameof(TaskRun.CreatedAt).ToSnakeCase();
		public static string StoppedAt { get; } = nameof(TaskRun.StoppedAt).ToSnakeCase();
		public static string CompletedAt { get; } = nameof(TaskRun.CompletedAt).ToSnakeCase();
		public static string RunnedAt { get; } = nameof(TaskRun.RunnedAt).ToSnakeCase();
		public static string OccurredAt { get; } = nameof(TaskRun.OccurredAt).ToSnakeCase();
		public static string ErrorMessage { get; } = nameof(TaskRun.ErrorMessage).ToSnakeCase();
	}
}