# Azure SQL Database Spark Connector

The official connector for Spark and Azure SQL Database.

This project provides a client library that allows your Azure SQL Database to be an input source or output sink for SparkJobs

## Requirements


## How to connect to Spark using this library
This connector uses Microsoft SQLServer JDBC driver to fetch data from/to the Azure SQL Database. 
Results are of the `DataFrame` type.

All connection properties in 
<a href="https://docs.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties"> 
  Microsoft JDBC Driver for SQL Server
</a> are supported in this connector. Add connection properties as fields in the `com.microsoft.azure.sqldb.spark.config.Config` object.

### Reading from Azure SQL Database using Scala
```scala 
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "mysqlserver.database.windows.net",
  "databaseName"   -> "MyDatabase",
  "dbTable"        -> "dbo.Clients"
  "user"           -> "username",
  "password"       -> "*********",
  "connectTimeout" -> "5", //seconds
  "queryTimeout"   -> "5"  //seconds
))

val collection = sqlContext.read.azureSQL(config)
collection.show()

```

### Writing to Azure SQL Database using Scala
```scala 
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
 
// Aquire a DataFrame collection (val collection)

val config = Config(Map(
  "url"          -> "mysqlserver.database.windows.net",
  "databaseName" -> "MyDatabase",
  "dbTable"      -> "dbo.Clients"
  "user"         -> "username",
  "password"     -> "*********"
))

import org.apache.spark.sql.SaveMode
collection.write.mode(SaveMode.Append).azureSQL(config)

```
### Pushdown query to Azure SQL Database using Scala
For SELECT queries with expected return results, please use 
[Reading from Azure SQL Database using Scala](#reading-from-azure-sql-database-using-scala)
```scala
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.query._
val query = """
              |UPDATE Customers
              |SET ContactName = 'Alfred Schmidt', City= 'Frankfurt'
              |WHERE CustomerID = 1;
            """.stripMargin

val config = Config(Map(
  "url"          -> "mysqlserver.database.windows.net",
  "databaseName" -> "MyDatabase",
  "user"         -> "username",
  "password"     -> "*********",
  "queryCustom"  -> query
))

sqlContext.azurePushdownQuery(config)
```
### Bulk Copy to Azure SQL Database / SQL Server using Scala
```scala
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

/** 
  Add column Metadata.
  If not specified, metadata will be automatically added
  from the destination table, which may suffer performance.
*/
var bulkCopyMetadata = new BulkCopyMetadata
bulkCopyMetadata.addColumnMetadata(1, "Title", java.sql.Types.NVARCHAR, 128, 0)
bulkCopyMetadata.addColumnMetadata(2, "FirstName", java.sql.Types.NVARCHAR, 50, 0)
bulkCopyMetadata.addColumnMetadata(3, "LastName", java.sql.Types.NVARCHAR, 50, 0)

val bulkCopyConfig = Config(Map(
  "url"               -> "mysqlserver.database.windows.net",
  "databaseName"      -> "MyDatabase",
  "user"              -> "username",
  "password"          -> "*********",
  "databaseName"      -> "zeqisql",
  "dbTable"           -> "dbo.Clients",
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

df.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)
//df.bulkCopyToSqlDB(bulkCopyConfig) if no metadata is specified.
```
## Active Directory / AccessToken authentication
Simply specify your config authentication to connect using ActiveDirectory. 
If not specified, default authentication method is server authentication.

```scala
val config = Config(Map(
  "url"                    -> "mysqlserver.database.windows.net",
  "databaseName"           -> "MyDatabase",
  "user"                   -> "username@microsoft.com",
  "password"               -> "*********",
  "authentication"         -> "ActiveDirectoryPassword",
  "trustServerCertificate" -> "true",
  "encrypt"                -> "true"
))

```


## Download

### Download from Maven

### Build this project
