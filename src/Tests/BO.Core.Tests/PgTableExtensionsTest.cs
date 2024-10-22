using BO.Core.Models;
using BO.Core.Extensions;
using BO.Core.Implementations;

namespace BO.Core.Tests;

public class PgTableExtensionsTest
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

	[Fact]
	public void GenerateAlterTableScript_ShouldBe_Correct() 
	{
		var newColumnDescriptor = new List<ColumnDescriptor> 
		{
			new ColumnDescriptor{ Field = "Id" },
			new ColumnDescriptor{ Field = "colum_1" },
			new ColumnDescriptor{ Field = "colum_2" }
		};

		var oldColumnDescriptors = new List<ColumnDescriptor> 
		{
			new ColumnDescriptor{ Field = "Id" },
			new ColumnDescriptor{ Field = "colum_3" }
		};

		var script = PgTableExtensions.GenerateAlterTableScript("test", oldColumnDescriptors, newColumnDescriptor);
	}
}