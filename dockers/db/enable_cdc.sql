use TestDB

GO

EXEC sp_changedbowner 'sa'

GO

EXEC sys.sp_cdc_enable_db ;

GO

EXEC sys.sp_cdc_enable_table 
   @source_schema = N'dbo', 
   @source_name = N'Order', 
   @capture_instance=N'dbo_Order_v1',
   @role_name = NULL;

GO

EXEC sys.sp_cdc_enable_table 
   @source_schema = N'dbo', 
   @source_name = N'OrderDetail', 
   @capture_instance=N'dbo_OrderDetail_v1',
   @role_name = NULL;