// Import libraries
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val url = "[Enter your url here]"
val databaseName = "[Enter your database name here]"
val dbTable = "[Enter your database table here]"

val user = "[Enter your username here]"
val password = "[Enter your password here]"

// Acquire data to be written. 
// df could be aquired in any way.
val localTable = "[Enter your local persisted table here]"
val df = spark.sql(s"SELECT * FROM $localTable")

val writeConfig = Config(Map(
  "url"               -> url,
  "databaseName"      -> databaseName,
  "dbTable"           -> dbTable,
  "user"              -> user, 
  "password"          -> password,
  "connectTimeout"    -> "5",
  "bulkCopyBatchSize" -> "100000",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

df.bulkCopyToSqlDB(writeConfig)

/**
For better performance, specify the column metadata of the table

var bulkCopyMetadata = new BulkCopyMetadata
bulkCopyMetadata.addColumnMetadata(1, "Title", java.sql.Types.NVARCHAR, 128, 0)
bulkCopyMetadata.addColumnMetadata(2, "FirstName", java.sql.Types.NVARCHAR, 128, 0)
bulkCopyMetadata.addColumnMetadata(3, "MiddleName", java.sql.Types.NVARCHAR, 128, 0)
bulkCopyMetadata.addColumnMetadata(4, "LastName", java.sql.Types.NVARCHAR, 128, 0)
..........

df.bulkCopyToSqlDB(writeConfig, bulkCopyMetadata)
**/