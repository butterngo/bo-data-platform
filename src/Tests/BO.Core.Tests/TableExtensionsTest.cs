using BO.Core.Extensions;
using BO.Core.Implementations;

namespace BO.Core.Tests
{
	public class TableExtensionsTest
	{
		[Fact]
		public async void ExtractColumnAsync_ShouldBe_Correct()
		{
			const string connectionString = "Host=localhost:5432;Database=grouparoo_docker;Username=postgres;Password=password;search path=bo_connectors;";

			var conn = new DataContext(connectionString).CreateConnection();

			var columns = await conn.ExtractColumnAsync(new 
			{
				table_schema = "northwind",
				table_name = "categories"
			});
		}
	}
}