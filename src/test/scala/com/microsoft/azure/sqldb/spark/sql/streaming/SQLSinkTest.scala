package com.microsoft.azure.sqldb.spark.sql.streaming

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._


class SQLSinkTest extends StreamTest with SharedSQLContext {
  import testImplicits._

  override val streamingTimeout: Span = 300.seconds
  val url = "localhost"
  val database = "test1"
  val user = "test"
  val password = "test"
  val dbTable = "dbo.testtable"
  val portNum = "58502"

  override def beforeAll(): Unit = {
    super.beforeAll()
    //TODO: Create SQL TEst Utils like EH does
  }

  override def afterAll(): Unit = {
    //Your code here
    super.afterAll()

  }

  private def createReader(testFileLocation: String): DataFrame = {
    //TODO: Prep test data
    val testschema = StructType(
      StructField("input", StringType) :: Nil)
    spark.readStream.schema(testschema).json(testFileLocation)
  }


  test("Structured Streaming - Write to Azure SQL DB with Connection String") {
    var config = Map(
      "url"          -> url,
      "databaseName" -> database,
      "user"         -> user,
      "password"     -> password,
      "dbTable"      -> dbTable,
      "portNumber"   -> portNum
    )
    var stream: DataStreamWriter[Row] = null
    val input = MemoryStream[String]
    input.addData("1", "2", "3", "4")
    var df = input.toDF().withColumnRenamed("value", "input")

    //val df = createReader("/sqltestdata")
    //TODO: Create Util functions to create SQL DB, table and truncate it and call the functions here
    withTempDir { checkpointDir =>
      config += ("checkpointLocation" -> checkpointDir.getCanonicalPath)  //Adding Checkpoint Location
      stream = df.writeStream
        .format("sqlserver")
        .options(config)
        .outputMode("Append")
    }
    var streamStart = stream.start()

    try {
      failAfter(streamingTimeout){
        Thread.sleep(100)
        //streamStart.processAllAvailable()
      }
      //var testDF = spark.read.sqlDB(Config(config)).as[String].select("value").map(_.toInt)
      checkDatasetUnorderly(spark.read.sqlDB(Config(config)).select($"input").as[String].map(_.toInt), 1, 2, 3, 4)
    } finally {
      streamStart.stop()
    }

    //val df: DataFrame =

    //TODO: Read data from SQL DB and check if it matches the data written (see EH implementation)
  }

  /*private def createWriter(inputDF: DataFrame, sqlConfig: AzureSQLConfig, withOutputMode: Option[OutputMode]): StreamingQuery = {
    inputDF.writeStream.format("azuresql").option("directCopy","true").option("")
  }*/


  test("Structured Streaming - Write to Azure SQL DB with variables defined"){

  }

  test("Structured Streaming - Write to Azure SQL DB with Connection String & variables defined"){

  }

  test("Structured Streaming - Incorrect username/password"){

  }

  test("Structured Streaming - Incorrect Server Name"){

  }

  test("Structured Streaming - Incorrect Database Name"){

  }

  test("Structured Streaming - Incomplete options defined"){

  }

  test("Structured Streaming - Incorrect Connection String"){

  }

  test("Structured Streaming - No Checkpoint location specified"){

  }

  test("Structured Streaming - Table does not exist"){

  }
}
