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
package com.microsoft.azure.sqldb.spark.query

import java.sql.{Connection, SQLException}

import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils._
import com.microsoft.azure.sqldb.spark.LoggingTrait
import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Implicit functions for SQLContext
  */
private[spark] case class QueryFunctions(@transient sqlContext: SQLContext) extends LoggingTrait {

  /**
    * Executes a custom query on the external database server which returns
    * either a dataframe or a boolean specifying successful/unsuccessful execution.
    *
    * @param config any general configuration
    * @return Either of DataFrame or Boolean based on query stated in config
    */
  def sqlDBQuery(config: Config): Either[DataFrame, Boolean] = {

    var connection: Connection = null

    val sql = config.get[String](SqlDBConfig.QueryCustom).getOrElse(
      throw new IllegalArgumentException("Query not found in QueryCustom in Config")
    )

    try {
      connection = getConnection(config)
      val statement = connection.createStatement()

      if (statement.execute(sql)) {
        Left(sqlContext.read.sqlDB(config))
      }
      else {
        Right(true)
      }
    }
    catch {
      case sqlException: SQLException => {
        sqlException.printStackTrace()
        Right(false)
      }
      case exception: Exception => {
        exception.printStackTrace()
        Right(false)
      }
    }
    finally {
      connection.close()
    }
  }
}
