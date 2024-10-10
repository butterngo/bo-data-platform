using System.Reflection;

namespace BO.Core;

public static class AssemblyHelpers
{
	public static IEnumerable<Type> GetAllTypesThatImplementInterface<T>(Assembly assembly)
	{
		return Assembly.GetExecutingAssembly().GetTypes()
					   .Where(type => typeof(T).IsAssignableFrom(type) && !type.IsInterface && !type.IsAbstract);
	}
}
