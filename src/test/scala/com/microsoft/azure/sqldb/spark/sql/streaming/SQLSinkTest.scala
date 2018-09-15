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
import com.microsoft.azure.sqldb.spark.utils.SQLServerTestUtils


class SQLSinkTest extends StreamTest with SharedSQLContext {
  import testImplicits._

  override val streamingTimeout: Span = 30.seconds
  val url = "localhost"
  val database = "test1"
  val user = "test"
  val password = "test"
  val dbTable = "dbo.newtesttable"
  val portNum = "58502"

  protected var SQLUtils: SQLServerTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLUtils = new SQLServerTestUtils
  }

  override def afterAll(): Unit = {
    if(SQLUtils != null) {
      SQLUtils.dropAllTables()
      SQLUtils = null
    }
    super.afterAll()
  }

  private def createReader(testFileLocation: String): DataFrame = {
    //TODO: Prep test data
    val testschema = StructType(
      StructField("input", StringType) :: Nil)
    spark.readStream.schema(testschema).json(testFileLocation)
  }


  test("Structured Streaming - Write to Azure SQL DB") {
    var config = Map(
      "url"          -> url,
      "databaseName" -> database,
      "user"         -> user,
      "password"     -> password,
      "dbTable"      -> dbTable,
      "portNumber"   -> portNum
    )
    val columns = Map(
      "input" -> "nvarchar(10)"
    )
    var success = SQLUtils.createTable(config, columns)
    if(!success){
      fail("Table creation failed. Please check your config")
    }
    var stream: DataStreamWriter[Row] = null
    val input = MemoryStream[String]
    input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    var df = input.toDF().withColumnRenamed("value", "input")

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
        streamStart.processAllAvailable()
      }
      checkDatasetUnorderly(spark.read.sqlDB(Config(config)).select($"input").as[String].map(_.toInt), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      success = SQLUtils.dropTable(config)
      if(!success){
        fail("Table deletion failed. Please check your config")
      }
      streamStart.stop()
    }
  }

  /*private def createWriter(inputDF: DataFrame, sqlConfig: AzureSQLConfig, withOutputMode: Option[OutputMode]): StreamingQuery = {
    inputDF.writeStream.format("azuresql").option("directCopy","true").option("")
  }*/


  test("Structured Streaming - Incorrect username/password ; Ensure the right error surfaces"){

  }

  test("Structured Streaming - Incorrect Server Name ; Ensure the right error surfaces"){

  }

  test("Structured Streaming - Incorrect Database Name ; Ensure the right error surfaces"){

  }

  test("Structured Streaming - Incomplete options defined ; Ensure the right error surfaces"){

  }

  test("Structured Streaming - Table does not exist ; Ensure the right error surfaces"){

  }
}
