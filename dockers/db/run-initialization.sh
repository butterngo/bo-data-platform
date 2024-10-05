 sleep 20s
 echo 'Starting setup script'
 printenv
 #run the setup script to create the DB and the schema in the DB
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Passw@rd -Q "RESTORE DATABASE TestDB FROM DISK = 'TestDB.bak'"
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Passw@rd -d TestDB -i enable_cdc.sql

 echo 'Finished setup script'