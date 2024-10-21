﻿using BO.Core.Entities;

namespace BO.Core.Interfaces;

public interface ITaskRunHandler : IDisposable
{
	Task HandleAsync(TaskRun taskRun, CancellationToken cancellationToken);
}

