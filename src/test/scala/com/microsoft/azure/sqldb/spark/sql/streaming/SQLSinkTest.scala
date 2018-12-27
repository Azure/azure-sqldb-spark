package com.microsoft.azure.sqldb.spark.sql.streaming

import java.sql.Timestamp

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
    if (SQLUtils != null) {
      SQLUtils.dropAllTables()
      SQLUtils = null
    }
    super.afterAll()
  }

  private def createReader(testFileLocation: String): DataFrame = {
    val testschema = StructType(
      StructField("input", StringType) :: Nil)
    spark.readStream.schema(testschema).json(testFileLocation)
  }


  test("Structured Streaming - Write to Azure SQL DB") {
    var config = Map(
      "url" -> url,
      "databaseName" -> database,
      "user" -> user,
      "password" -> password,
      "dbTable" -> dbTable,
      "portNumber" -> portNum
    )
    val columns = Map(
      "input" -> "nvarchar(10)"
    )
    var success = SQLUtils.createTable(config, columns)
    if (!success) {
      fail("Table creation failed. Please check your config")
    }
    var stream: DataStreamWriter[Row] = null
    val input = MemoryStream[String]
    input.addData("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    var df = input.toDF().withColumnRenamed("value", "input")

    withTempDir { checkpointDir =>
      config += ("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
      stream = df.writeStream
        .format("sqlserver")
        .options(config)
        .outputMode("Append")
    }
    var streamStart = stream.start()

    try {
      failAfter(streamingTimeout) {
        streamStart.processAllAvailable()
      }
      checkDatasetUnorderly(spark.read.sqlDB(Config(config)).select($"input").as[String].map(_.toInt), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      success = SQLUtils.dropTable(config)
      if (!success) {
        fail("Table deletion failed. Please check your config")
      }
      streamStart.stop()
    }
  }


  test("Structured Streaming - Incorrect username/password ; Ensure the right error surfaces") {

  }

  test("Structured Streaming - Incorrect Server Name ; Ensure the right error surfaces") {

  }

  test("Structured Streaming - Incorrect Database Name ; Ensure the right error surfaces") {

  }

  test("Structured Streaming - Incomplete options defined ; Ensure the right error surfaces") {

  }

  test("Structured Streaming - Table does not exist ; Ensure the right error surfaces") {

  }

  test("Structured Streaming - Check Data Type Translation") {
    var config = Map(
      "url" -> url,
      "databaseName" -> database,
      "user" -> user,
      "password" -> password,
      "dbTable" -> dbTable,
      "portNumber" -> portNum
    )
    val columns = Map(
      //"binType" -> "varbinary", //TODO: Test for Longvarbinary as well
      "boolType" -> "bit", //TODO: Test for Bool as well
     // "byteType" -> "tinyint",
      "dblType" -> "float",
      "fltType" -> "float",
      "intType" -> "int",
    //  "longType" -> "bigint",
    //  "shortType" -> "smallint",
      "strType" -> "nvarchar(10)"//,
     // "tsType" -> "timestamp"
    )



    case class ColClass(boolType: Boolean/*, byteType: Byte*/, dblType: Double, fltType: Float, intType: Integer, /*longType: Long, shortType: Short,*/ strType: String/*, tsType: java.sql.Timestamp*/)

    var success = SQLUtils.createTable(config, columns)
    if (!success) {
      fail("Table creation failed. Please check your config")
    }
    var stream: DataStreamWriter[Row] = null

    val input = MemoryStream[String]
    input.addData(
      "true,2.333333333333,-123,10.99992,test",//,2018-08-10 17:18:20",
      "false,2.333333333333,100,1012300.012345,testrow2"//,1945-01-01 00:01:01"
     // ColClass(false, 1, 2.333333333333, 100, 1012300, 13445920L, -10000, "test", new Timestamp(2018, 8, 10, 17, 18, 20, 11111))
    )

    /*
    "true, 1, 2.333333333333, -123, 10, 123120L, -20000, test", //2018-08-10 17:18:20",
      "false, 1, 2.333333333333, 100, 1012300, 13445920L, -10000, test"//, 1945-01-01 00:01:01"
     */

    //val newNames = Seq("boolType"/*, "byteType"*/, "dblType", "fltType", "intType", /*"longType", "shortType",*/ "strType", "tsType")
    var df = input.toDF.selectExpr(
      "cast(split(value,',')[0] as boolean) boolType",
      //"cast(split(value,'[,]')[1] as byte) as byteType",
      "cast(split(value,',')[1] as double) dblType",
      "cast(split(value,',')[2] as int) intType",
      "cast(split(value,',')[3] as float) fltType",
      //"cast(split(value,'[,]')[5] as bigint) as longType",
     // "cast(split(value,'[,]')[6] as smallint) as shortType",
      "cast(split(value,',')[4] as string) strType"//,
     // "cast(unix_timestamp(split(value,',')[5], 'yyyy-MM-dd hh:mm:ss') as timestamp) as tsType"
    )

    withTempDir { checkpointDir =>
      config += ("checkpointLocation" -> checkpointDir.getCanonicalPath) //Adding Checkpoint Location
      stream = df.writeStream
        .format("sqlserver")
        .options(config)
        .outputMode("Append")
    }
    var streamStart = stream.start()

    try {
      failAfter(streamingTimeout) {
        streamStart.processAllAvailable()
      }

      val checkData = spark.read.sqlDB(Config(config)).select($"intType", $"strType")
      checkDatasetUnorderly(checkData.as[(Int, String)], (-123, "test"),(100, "testrow2"))
      //checkDatasetUnorderly(checkData.as[(Boolean, Byte, Double, Float, Int, Long, Short, String, java.sql.Timestamp)], (true, 1.asInstanceOf[Byte], 2.333333333333, -123.asInstanceOf[Float], 10, 123120L, -20000.asInstanceOf[Short], "test", "2018-08-10 17:18:20".asInstanceOf[Timestamp]),(false, 1.asInstanceOf[Byte], 2.333333333333, 100.asInstanceOf[Float], 1012300, 13445920L, -10000.asInstanceOf[Short], "test", "1945-01-01 00:01:01".asInstanceOf[Timestamp]))

      //checkDatasetUnorderly(spark.read.sqlDB(Config(config)).select($"input").as[ColClass], ColClass(true, 1, 2.333333333333, -123, 10, 123120L, -20000, "test", new Timestamp(2018, 8, 10, 17, 18, 20, 11111)),
      //  ColClass(false, 1, 2.333333333333, 100, 1012300, 13445920L, -10000, "test", new Timestamp(2018, 8, 10, 17, 18, 20, 11111)))
    }
    finally {
      success = SQLUtils.dropTable(config)
      if (!success) {
        fail("Table deletion failed. Please check your config")
      }
      streamStart.stop()
    }
  }
}//Check error and then run test
