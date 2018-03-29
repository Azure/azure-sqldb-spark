// Import libraries
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val url = "[Enter your url here]"
val databaseName = "[Enter your database name here]"
val dbTable = "[Enter your database table here]"

val user = "[Enter your username here]"
val password = "[Enter your password here]"

// READ FROM CONFIG
val readConfig = Config(Map(
  "url"            -> url,
  "databaseName"   -> databaseName,
  "user"           -> user,
  "password"       -> password,
  "connectTimeout" -> "5",
  "queryTimeout"   -> "5",
  "dbTable"        -> dbTable
))

val df = sqlContext.read.sqlDB(readConfig)
println("Total rows: " + df.count)
df.show()

// TRADITIONAL SYNTAX
import java.util.Properties

val properties = new Properties()
properties.put("databaseName", databaseName)
properties.put("user", user)
properties.put("password", password)
properties.put("connectTimeout", "5")
properties.put("queryTimeout", "5")

val df = sqlContext.read.sqlDB(url, dbTable, properties)
println("Total rows: " + df.count)
df.show()
