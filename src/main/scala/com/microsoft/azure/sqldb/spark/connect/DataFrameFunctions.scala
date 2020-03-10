/**
  * The MIT License (MIT)
  * Copyright (c) 2018 Microsoft Corporation
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package com.microsoft.azure.sqldb.spark.connect

import java.sql.{Connection, SQLException}

import com.microsoft.azure.sqldb.spark.bulkcopy.{BulkCopyMetadata, SQLServerBulkDataFrameFileRecord}
import com.microsoft.azure.sqldb.spark.LoggingTrait
import com.microsoft.azure.sqldb.spark.bulk.BulkCopyUtils
import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try

/**
  * Implicit functions for DataFrame
  */
private[spark] case class DataFrameFunctions[T](@transient dataFrame: DataFrame) extends LoggingTrait {

  /**
    * Saves the contents of the [[DataFrame]]
    * to Azure SQL DB or SQL Server through the Bulk Copy API
    *
    * @param config the database connection properties and bulk copy properties
    * @param metadata the metadata of the columns - will be null if not specified
    */
  def bulkCopyToSqlDB(config: Config, metadata: BulkCopyMetadata = null, createTable:Boolean = false): Unit = {
    // Ensuring the table exists in the DB already
    if(createTable) {
      dataFrame.limit(0).write.sqlDB(config)
    }

    val actualMetadata = if(metadata == null) {
      getConnectionOrFail(config).recover({
        case e: ClassNotFoundException =>
          logError("JDBC driver not found in class path", e)
          throw e
        case e1: SQLException =>
          logError("Connection cannot be established to the database", e1)
          throw e1
      }).flatMap(conn => {
        inferBulkCopyMetadata(config, conn)
      }).recover({
        case e: SQLException =>
          logError("Column metadata not specified and cannot retrieve metadata from database", e)
        throw e
      }).get
    } else {
      metadata
    }
    dataFrame.foreachPartition((iterator: Iterator[Row]) => bulkCopy(config, iterator, actualMetadata))
  }

  private def getConnectionOrFail(config:Config):Try[Connection] = {
    Try {
      ConnectionUtils.getConnection(config)
    }
  }

  private def inferBulkCopyMetadata(config: Config, connection:Connection):Try[BulkCopyMetadata] = {
    val dbTable = config.get[String](SqlDBConfig.DBTable).get
    Try {
      val resultSetMetaData = BulkCopyUtils.getTableColumns(dbTable, connection)
      BulkCopyUtils.createBulkCopyMetadata(resultSetMetaData)
    }
  }

  /**
    * Uses the Bulk Copy API to copy contents of a dataframe partition to an
    * external database table.
    *
    * @param config any write configuration with the specified properties.
    * @param iterator an iterator for a dataframe partition.
    * @param metadata User specified bulk copy metadata.
    */
  private def bulkCopy(config: Config, iterator: Iterator[Row], metadata: BulkCopyMetadata): Unit = {
    var connection: Connection = null
    try {
      connection = ConnectionUtils.getConnection(config)
    } catch {
      case e: ClassNotFoundException =>
        logError("JDBC driver not found in class path", e)
        throw e
      case e1: SQLException =>
        logError("Connection cannot be established to the database", e1)
        throw e1
    }

    val dbTable = config.get[String](SqlDBConfig.DBTable).get

    // Retrieves column metadata from external database table if user does not specify.
    val bulkCopyMetadata = metadata

    var committed = false
    val supportsTransactions = BulkCopyUtils.getTransactionSupport(connection)
    try {
      if (supportsTransactions){
        connection.setAutoCommit(false)
      }

      val fileRecord = new SQLServerBulkDataFrameFileRecord(iterator, bulkCopyMetadata)
      val sqlServerBulkCopy = new SQLServerBulkCopy(connection)

      sqlServerBulkCopy.setDestinationTableName(dbTable)
      sqlServerBulkCopy.setBulkCopyOptions(BulkCopyUtils.getBulkCopyOptions(config))
      sqlServerBulkCopy.writeToServer(fileRecord)

      if (supportsTransactions){
        connection.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        if (!committed && supportsTransactions){
          logError("An error occurred while writing to database, attempting rollback", e)
        }
        throw e
    } finally {
      if (!committed){
        if (supportsTransactions){
          connection.rollback()
        }
        connection.close()
      } else {
        try {
          connection.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }
}
