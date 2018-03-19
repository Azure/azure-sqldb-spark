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

import java.util.Properties

import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils._
import com.microsoft.azure.sqldb.spark.LoggingTrait
import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import org.apache.spark.sql.DataFrameWriter

/**
  * Implicit functions for DataFrameWriter
  */
private[spark] case class DataFrameWriterFunctions(@transient writer: DataFrameWriter[_]) extends LoggingTrait {

  /**
    * Saves the contents of the `DataFrame` to Azure SQL dB or SQL Server.
    *
    * @param writeConfig the [[com.microsoft.azure.sqldb.spark.config.Config]] to use
    */
  def sqlDB(writeConfig: Config): Unit = {
    val url = writeConfig.get[String](SqlDBConfig.URL).get
    val properties = createConnectionProperties(writeConfig)
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(
      throw new IllegalArgumentException("Table not found in DBTable in Config")
    )

    sqlDB(url, table, properties)
  }

  /**
    * Saves the contents of the `DataFrame` to Azure SQL dB.
    *
    * @param url the url of the server
    * @param table the database table being written to.
    * @param properties any additional connection properties handled by the jdbc driver
    */
  def sqlDB(url: String, table: String, properties: Properties): Unit = writer.jdbc(createJDBCUrl(url), table, properties)

}

