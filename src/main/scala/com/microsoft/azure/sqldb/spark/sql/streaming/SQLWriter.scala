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

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.catalyst.InternalRow
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils._
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Attribute

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

  //TODO: Clean this up and make it similar to that of SQL DB
    var writeConfig = Config(parameters)
    val url = writeConfig.get[String](SqlDBConfig.URL).get    //TODO: If URL is not specified, try to construct one
    val db = writeConfig.get[String](SqlDBConfig.DatabaseName).get
    val properties = createConnectionProperties(writeConfig)
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
    if(connection.isClosed){
      val test = 1
    }

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
      var schemaDatatype = new ListBuffer[String]()
      var colNames = ""
      var values = ""

      if(schema.size > 0) {
        schema.foreach(col => {
          colNames += col.name + ","
          values += "?,"
          schemaDatatype+= col.dataType.toString()
        })
      }

    var sql = "INSERT INTO " + table + " (" + colNames.substring(0, colNames.length-1) + " , partitionid" + ")" + " VALUES (" + values.substring(0, values.length-1) + ",?" + ");"

    queryExecution.toRdd.foreachPartition(iter => {
      val ps = connection.prepareStatement(sql)
      iter.foreach(row => {
        val pid = TaskContext.get().partitionId()
        if(ps != null) {
          try{
            var i = 0
            for(e <- schemaDatatype) {
              val testVar = row.getString(i)
              println("Value: " + testVar + " ; i: " + i)
              e match {
                case "ByteType" => ps.setByte(i+1, row.getByte(i))
                case "ShortType" => ps.setShort(i+1,row.getShort(i))
                case "IntegerType" => ps.setInt(i+1, row.getInt(i))
                case "LongType" => ps.setLong(i+1, row.getLong(i))
                case "FloatType" => ps.setFloat(i+1, row.getFloat(i))
                case "DoubleType" => ps.setDouble(i+1, row.getDouble(i))
                //      case "DecimalType" => statement.setBigDecimal(i, )   //TODO: try to use getAccessor and find a similar method in statement
                case "StringType" => ps.setString(i+1, row.getString(i))
                case "BinaryType" => ps.setBytes(i+1, row.getBinary(i))
                case "BooleanType" => ps.setBoolean(i+1, row.getBoolean(i))
                //      case "TimestamType" => statement.setTimestamp(i+1, row.get.getTimestamp(i))
                //      case "DateType" => statement.setDate(i+1, row.getDate(i))
              }

              i += 1
            }
            ps.setInt(2, pid)
            ps.execute()
          } catch {
            case e: SQLException => log.error("Error writing to SQL DB on row: " + row.toString())
              throw e   //TODO: Give users the option to abort or continue
          }
        }
        //streamToDB(sql, row, schemaDatatype)
      })
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

    //TODOv2: Provide the option to the user to define the columns they're writing to and/or column mapping
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
                  ps: PreparedStatement,
                  sql: String,
                  row: InternalRow,
                  colDataTypes: ListBuffer[String]
                ): Unit = {

    if(ps == null) {
     // ps = connection.prepareStatement(sql)
    }


    if(ps != null) {
      try{
        for(i <- 0 to colDataTypes.length-1) {
          colDataTypes(i) match {
            case "ByteType" => ps.setByte(i+1, row.getByte(i))
            case "ShortType" => ps.setShort(i+1,row.getShort(i))
            case "IntegerType" => ps.setInt(i+1, row.getInt(i))
            case "LongType" => ps.setLong(i+1, row.getLong(i))
            case "FloatType" => ps.setFloat(i+1, row.getFloat(i))
            case "DoubleType" => ps.setDouble(i+1, row.getDouble(i))
      //      case "DecimalType" => statement.setBigDecimal(i, )   //TODO: try to use getAccessor and find a similar method in statement
            case "StringType" => ps.setString(i+1, row.getString(i))
            case "BinaryType" => ps.setBytes(i+1, row.getBinary(i))
            case "BooleanType" => ps.setBoolean(i+1, row.getBoolean(i))
      //      case "TimestamType" => statement.setTimestamp(i+1, row.get.getTimestamp(i))
      //      case "DateType" => statement.setDate(i+1, row.getDate(i))
          }
        }
        ps.execute()
      } catch {
        case e: SQLException => log.error("Error writing to SQL DB on row: " + row.toString())
          throw e   //TODO: Give users the option to abort or continue
      }
    }

  }

  /*
  Prepares the Insert statement and calls the [[SqlJDBCWrapper.executeCmd]]
   */
  /*
  def loadSqlData(
                   conn: Connection,
                   subset: Boolean,
                   conf: AzureSQLConfig,
                   data: DataFrame
                 ): Unit = {
    if(subset){
      var schemaStr:String = ""
      val schema = data.schema
      schema.fields.foreach{
        schemaStr += _.name + ","
      }
      schemaStr.substring(0,schemaStr.length-2)
      //val insertStatement = s"INSERT INTO $table ("+ schemaStr + ") VALUES ("
      //TODO: Handle append mode
    } else {
      val connectionProperties = new Properties()
      connectionProperties.put("user", conf.username)
      connectionProperties.put("password", conf.password)
      connectionProperties.put("driver", DRIVER_NAME)
      try{
        data.write.mode(SaveMode.Append).jdbc(conf.connectionString, conf.tableName, connectionProperties)
      } catch {
        case e: Exception =>
          log.error("Error writing batch data to SQL DB. Error details: "+ e)
          throw e
      }
    }
  } */
}


/*
val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


//Creating Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"$username")
    connectionProperties.put("password", s"$password")
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    //TODO: Write data to SQL DB

    val serverName = parameters.get(SERVER_KEY).map(_.trim)
    val portNumber = parameters.get(PORT_KEY).map(_.trim).flatMap(s => Try(s.toInt).toOption)
    val database = parameters.get(DB_KEY).map(_.trim)
    val username = parameters.get(USER_KEY).map(_.trim)
    val password = parameters.get(PWD_KEY).map(_.trim)
    val tableName = parameters.get(TABLE_KEY).map(_.trim)


 */