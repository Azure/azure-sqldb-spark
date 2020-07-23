Updated Jun 2020: This project is not being actively maintained. Instead, [Apache Spark Connector for SQL Server and Azure SQL](https://github.com/microsoft/sql-spark-connector) is now available, with support for Python and R bindings, an easier-to use interface to bulk insert data, and many other improvements. We encourage you to actively evaluate and use the new connector.

# Spark connector for Azure SQL Databases and SQL Server

[![Build Status](https://travis-ci.org/Azure/azure-sqldb-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-sqldb-spark)

The Spark connector for [Azure SQL Database](https://azure.microsoft.com/en-us/services/sql-database/) and [SQL Server](https://www.microsoft.com/en-us/sql-server/default.aspx) enables SQL databases, including Azure SQL Databases and SQL Server, to act as input data source or output data sink for Spark jobs. It allows you to utilize real time transactional data in big data analytics and persist results for adhoc queries or reporting. 

Comparing to the built-in Spark connector, this connector provides the ability to bulk insert data into SQL databases. It can outperform row by row insertion with 10x to 20x faster performance. The Spark connector for Azure SQL Databases and SQL Server also supports AAD authentication. It allows you securely connecting to your Azure SQL databases from Azure Databricks using your AAD account. It provides similar interfaces with the built-in JDBC connector. It is easy to migrate your existing Spark jobs to use this new connector.

## How to connect to Spark using this library
This connector uses Microsoft SQLServer JDBC driver to fetch data from/to the Azure SQL Database. 
Results are of the `DataFrame` type.

All connection properties in 
<a href="https://docs.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties"> 
  Microsoft JDBC Driver for SQL Server
</a> are supported in this connector. Add connection properties as fields in the `com.microsoft.azure.sqldb.spark.config.Config` object.

  
### Reading from Azure SQL Database or SQL Server
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

val collection = sqlContext.read.sqlDB(config)
collection.show()

```

### Writing to Azure SQL Database or SQL Server
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
collection.write.mode(SaveMode.Append).sqlDB(config)

```
### Pushdown query to Azure SQL Database or SQL Server
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

sqlContext.sqlDBQuery(config)
```
### Bulk Copy to Azure SQL Database or SQL Server
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
  "databaseName"      -> "MyDatabase",
  "dbTable"           -> "dbo.Clients",
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

df.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)
//df.bulkCopyToSqlDB(bulkCopyConfig) if no metadata is specified.
```

## Requirements
Official supported versions

| Component | Versions Supported |
| --------- | ------------------ |
| Apache Spark | 2.0.2 or later |
| Scala | 2.10 or later |
| Microsoft JDBC Driver for SQL Server | 6.2 to 7.4 ^|
| Microsoft SQL Server | SQL Server 2008 or later |
| Azure SQL Databases | Supported |

^ Driver version 8.x not tested

## Download
### Download from Maven
You can download the latest version from [here](https://search.maven.org/search?q=a:azure-sqldb-spark)

You can also use the following coordinate to import the library into Azure SQL Databricks:
com.microsoft.azure:azure-sqldb-spark:1.0.2

### Build this project
Currently, the connector project uses maven. To build the connector without dependencies, you can run:
```sh
mvn clean package
```

## Contributing & Feedback

This project has adopted the [Microsoft Open Source Code of
Conduct](https://opensource.microsoft.com/codeofconduct/).  For more information
see the [Code of Conduct
FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional
questions or comments.

To give feedback and/or report an issue, open a [GitHub
Issue](https://help.github.com/articles/creating-an-issue/).


*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.*
