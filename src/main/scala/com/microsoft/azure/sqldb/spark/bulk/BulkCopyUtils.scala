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
package com.microsoft.azure.sqldb.spark.bulk

import java.sql.{Connection, ResultSetMetaData}

import com.microsoft.azure.sqldb.spark.LoggingTrait
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions

import scala.util.control.NonFatal

/**
  * Helper and utility methods used for setting up a Bulk Copy transaction.
  */
private[spark] object BulkCopyUtils extends LoggingTrait {

  /**
    * Database table columns start at index 1.
    */
  val ColumnStartingIndex = 1

  /**
    * Extracts column names, types, precision and scale from a [[ResultSetMetaData]]
    * and creates a [[BulkCopyMetadata]] object.
    *
    * @param resultSetMetaData metadata from an external database table.
    * @return BulkCopyMetadata with the extracted column metadata.
    */
  def createBulkCopyMetadata(resultSetMetaData: ResultSetMetaData): BulkCopyMetadata = {
    val bulkCopyMetadata = new BulkCopyMetadata

    for (column <- ColumnStartingIndex to resultSetMetaData.getColumnCount) {
      bulkCopyMetadata.addColumnMetadata(
        column,
        resultSetMetaData.getColumnName(column),
        resultSetMetaData.getColumnType(column),
        resultSetMetaData.getPrecision(column),
        resultSetMetaData.getScale(column)
      )
    }

    bulkCopyMetadata
  }

  /**
    * Extracts Bulk Copy properties from Config and creates [[SQLServerBulkCopyOptions]].
    * Will use default values if not specified.
    *
    * @param config the Config object with specified bulk copy properties
    * @return [[SQLServerBulkCopyOptions]] for the JDBC Bulk Copy API
    */
  def getBulkCopyOptions(config: Config): SQLServerBulkCopyOptions = {
    val copyOptions = new SQLServerBulkCopyOptions

    copyOptions.setBatchSize(
      config.get[String](SqlDBConfig.BulkCopyBatchSize)
        .getOrElse(SqlDBConfig.BulkCopyBatchSizeDefault.toString).toInt
    )
    copyOptions.setBulkCopyTimeout(
      config.get[String](SqlDBConfig.BulkCopyTimeout)
        .getOrElse(SqlDBConfig.BulkCopyTimeoutDefault.toString).toInt
    )
    copyOptions.setCheckConstraints(
      config.get[String](SqlDBConfig.BulkCopyCheckConstraints)
        .getOrElse(SqlDBConfig.BulkCopyCheckConstraintsDefault.toString).toBoolean
    )
    copyOptions.setFireTriggers(
      config.get[String](SqlDBConfig.BulkCopyFireTriggers)
        .getOrElse(SqlDBConfig.BulkCopyFireTriggersDefault.toString).toBoolean
    )
    copyOptions.setKeepIdentity(
      config.get[String](SqlDBConfig.BulkCopyKeepIdentity)
        .getOrElse(SqlDBConfig.BulkCopyKeepIdentityDefault.toString).toBoolean
    )
    copyOptions.setKeepNulls(
      config.get[String](SqlDBConfig.BulkCopyKeepNulls)
        .getOrElse(SqlDBConfig.BulkCopyKeepNullsDefault.toString).toBoolean
    )
    copyOptions.setTableLock(
      config.get[String](SqlDBConfig.BulkCopyTableLock)
        .getOrElse(SqlDBConfig.BulkCopyTableLockDefault.toString).toBoolean
    )
    copyOptions.setUseInternalTransaction(
      config.get[String](SqlDBConfig.BulkCopyUseInternalTransaction)
        .getOrElse(SqlDBConfig.BulkCopyUseInternalTransactionDefault.toString).toBoolean
    )
    copyOptions.setAllowEncryptedValueModifications(
      config.get[String](SqlDBConfig.BulkCopyAllowEncryptedValueModifications)
        .getOrElse(SqlDBConfig.BulkCopyAllowEncryptedValueModificationsDefault.toString).toBoolean
    )

    copyOptions
  }

  /**
    * Retrieves table columns and metadata from remote database
    *
    * @param table the table to retrieve column metadata
    * @param connection the active JDBC connection
    * @return the [[ResultSetMetaData]] of the executed query.
    */
  def getTableColumns(table: String, connection: Connection): ResultSetMetaData = {
    // A bit hacky, but this is the most efficient way.
    val statement = s"SELECT TOP 0 * FROM $table"

    connection.createStatement().executeQuery(statement).getMetaData
  }

  /**
    * Retrieves transaction support from remote database
    *
    * @param connection the active JDBC connection
    * @return true if the connected database support transactions, false otherwise
    */
  def getTransactionSupport(connection: Connection): Boolean ={
    var isolationLevel = Connection.TRANSACTION_NONE
    try {
      val metadata = connection.getMetaData
      if (metadata.supportsTransactions){
        isolationLevel = metadata.getDefaultTransactionIsolation
      }
    } catch {
      case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
    }

    isolationLevel != Connection.TRANSACTION_NONE
  }
}
