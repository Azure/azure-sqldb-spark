// Import libraries
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

// WRITE FROM CONFIG
val writeConfig = Config(Map(
  "url"            -> url,
  "databaseName"   -> databaseName,
  "dbTable"        -> dbTable,
  "user"           -> user, 
  "password"       -> password,
  "connectTimeout" -> "5",
  "queryTimeout"   -> "5"
))

df.write.mode(SaveMode.Append).sqlDB(writeConfig)

/** TRADITIONAL SYNTAX

import java.util.Properties

val properties = new Properties()

properties.put("databaseName", databaseName)
properties.put("user", user)
properties.put("password", password)
properties.put("connectTimeout", "5")
properties.put("queryTimeout", "5")

df.write.mode(SaveMode.Append).sqlDB(url, dbTable, properties)

**/