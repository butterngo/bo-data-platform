using Avro.Generic;
using Paillave.Etl.Core;

namespace BO.PG.SinkConnector.ValuesProviders;

internal class PostgresqlValueProvider : ValuesProviderBase<GenericRecord, GenericRecord>
{
	public override ProcessImpact PerformanceImpact => ProcessImpact.Light;

	public override ProcessImpact MemoryFootPrint => ProcessImpact.Light;

	public override void PushValues(GenericRecord input, Action<GenericRecord> push, CancellationToken cancellationToken, IDependencyResolver resolver, IInvoker invoker)
	{
		push(input);
	}
}
