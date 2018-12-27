/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.sqldb.spark.sql.streaming

import java.sql._
import java.text.SimpleDateFormat

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.catalyst.InternalRow
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils._
import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions._

import scala.collection.mutable.ListBuffer

/**
  * The [[SQLWriter]] class is used to write data from a batch query
  * or structured streaming query, given by a [[QueryExecution]], to Azure SQL Database or Azure SQL Data Warehouse.
  */
private[spark] object SQLWriter extends Logging {

  var subset = false //This variable identifies whether you want to write to all columns of the SQL table or just select few.
  var DRIVER_NAME: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  var connection: Connection = _
  //var ps: PreparedStatement = null

  override def toString: String = "SQLWriter"

  def write(
             sparkSession: SparkSession,
             data: DataFrame,
             queryExecution: QueryExecution,
             saveMode: SaveMode,
             parameters: Map[String, String]
           ): Unit = {

  //TODO: Clean up all the commented code and break it down into methods
  //TODO: Provide the option to the user to define the columns they're writing to and/or column mapping
    var writeConfig = Config(parameters)
    val url = writeConfig.get[String](SqlDBConfig.URL).get    //TODO: If URL is not specified, try to construct one
    val db = writeConfig.get[String](SqlDBConfig.DatabaseName).get
    var createTable = false
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(
      //createTable = true
      throw new IllegalArgumentException("Table not found in DBTable in Config")
    )
    val user = writeConfig.get[String](SqlDBConfig.User).get
    val password = writeConfig.get[String](SqlDBConfig.Password).get
    val port = writeConfig.get[String](SqlDBConfig.PortNumber).getOrElse("1433")

    //Using regular write
    Class.forName(DRIVER_NAME)
    connection = DriverManager.getConnection(createJDBCUrl(url, Some(port))+";database="+db, user, password)

    //TODO: Using Bulk Copy
    /*val bulkCopyConfig = Config(Map(
      "url" -> url,
      "databaseName" -> writeConfig.get[String](SqlDBConfig.DatabaseName).get,
      "user" -> writeConfig.get[String](SqlDBConfig.User).get,
      "password" -> writeConfig.get[String](SqlDBConfig.DatabaseName).get,
      "dbTable" -> table
    ))
    try{
     data.bulkCopyToSqlDB()
    } catch {
      case e: Exception =>
        log.error("Error writing batch data to SQL DB. Error details: "+ e)
        throw e
    }
    */

    /* Getting Column information */
    try{

      val schema = queryExecution.analyzed.output
      var schemaDatatype = new ListBuffer[DataType]()
      var accessorsArr = new ListBuffer[(SpecializedGetters, Int) => Any]()
      //var schemaDatatype = new ListBuffer[String]()
      var colNames = ""
      var values = ""

      if(schema.size > 0) {
        schema.foreach(col => {
          colNames += col.name + ","
          values += "?,"
          schemaDatatype+= col.dataType
          accessorsArr += getAccessor(col.dataType)
          //schemaDatatype+= col.dataType.toString()
        })
      }

      var sql = "INSERT INTO " + table + " (" + colNames.substring(0, colNames.length-1) + ")" + " VALUES (" + values.substring(0, values.length-1) + ");"

      queryExecution.toRdd.foreachPartition(iter => {
        streamToDB(iter, sql, accessorsArr)
      })
    } catch {
      case e: Exception =>
        log.error("Error writing batch data to SQL DB. Error details: "+ e)
        throw e
    } finally {
    //  connection.close()
    }



    /*
      val table = sqlConf.tableName
      val jdbcWrapper: SqlJDBCWrapper = new SqlJDBCWrapper

      //TODO: Remove the error below.
      throw new SQLException("Currently only directCopy supported")

      connection = jdbcWrapper.setupConnection(sqlConf.connectionString, sqlConf.username, sqlConf.password, Option(DRIVER_NAME))   //TODOv2: Hard-coding the driver name for now
      connection.setAutoCommit(false)
      val mappedData = dataMapper(connection, table, data) //TODO: Handle data type conversions smoothly & check table existence, checks column names and types and map them to dataframe columns
      val schema = mappedData.schema
      if(data.schema == mappedData.schema){     //TODOv2: Instead of this, read from the params, so we don't have to call jdbcWrapper or dataMapper.
        subset = true;
      }
      loadSqlData(connection, subset, sqlConf, mappedData)
      connection.commit()


    //TODOv2: Handle creation of tables and append/overwrite of tables ; for v1, only append mode
    */
  }

  /*def createPreparedStatement(
                            conn: Connection,
                            schema: Seq[Attribute]
               ): PreparedStatement = {
    
  }*/

  /*
  This method checks to see if the table exists in SQL. If it doesn't, it creates the table. This method also ensures that the data types of the data frame are compatible with that of Azure SQL database. If they aren't, it converts them and returns the converted data frame
   */
  def streamToDB(
                  iter: Iterator[InternalRow],
                  sql: String,
                  //schemaDatatypes: ListBuffer[DataType]
                  accessorsArr: ListBuffer[(SpecializedGetters, Int) => Any]
                ): Unit = {

    val ps = connection.prepareStatement(sql)
    val dtFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    iter.foreach(row => {
      if(ps != null) {
        try{
          var i = 0
          for(accessor <- accessorsArr) {
            var data = accessor(row, i)
            ps.setObject(i+1, data)
            i += 1
          }
          ps.execute()
        } catch {
          case e: SQLException => log.error("Error writing to SQL DB on row: "/* + row.toString()*/)
            //throw e   //TODO 2: Skip record if it fails
        }
      } else {
        log.error("Error: PreparedStatement is null")
      }
    })

  }

  //Copied from Spark 2.4 (InternalRow.scala class)
  def getAccessor(dataType: DataType): (SpecializedGetters, Int) => Any = dataType match {
    case BooleanType => (input, ordinal) => input.getBoolean(ordinal)
    case ByteType => (input, ordinal) => input.getByte(ordinal)
    case ShortType => (input, ordinal) => input.getShort(ordinal)
    case IntegerType | DateType => (input, ordinal) => input.getInt(ordinal)
    case LongType | TimestampType => (input, ordinal) => input.getLong(ordinal)
    case FloatType => (input, ordinal) => input.getFloat(ordinal)
    case DoubleType => (input, ordinal) => input.getDouble(ordinal)
    case StringType => (input, ordinal) => input.getUTF8String(ordinal)
    case BinaryType => (input, ordinal) => input.getBinary(ordinal)
    case CalendarIntervalType => (input, ordinal) => input.getInterval(ordinal)
    case t: DecimalType => (input, ordinal) => input.getDecimal(ordinal, t.precision, t.scale)
    case t: StructType => (input, ordinal) => input.getStruct(ordinal, t.size)
    case _: ArrayType => (input, ordinal) => input.getArray(ordinal)
    case _: MapType => (input, ordinal) => input.getMap(ordinal)
    //case u: UserDefinedType[_] => getAccessor(u.sqlType)
    case _ => (input, ordinal) => input.get(ordinal, dataType)
  }
}


/*val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"*/