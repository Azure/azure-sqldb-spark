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
import org.apache.spark.sql.{DataFrame, DataFrameReader}

private[spark] case class DataFrameReaderFunctions(@transient reader: DataFrameReader) extends LoggingTrait {

  /**
    * Creates a [[DataFrame]] based on the read configuration properties.
    *
    * @param readConfig any read configuration.
    * @return DataFrame
    */
  def sqlDB(readConfig: Config): DataFrame = {
    reader.jdbc(
      createJDBCUrl(readConfig.get[String](SqlDBConfig.URL).get),
      getTableOrQuery(readConfig),
      createConnectionProperties(readConfig)
    )
  }

  /**
    * Creates a [[DataFrame]] based on the url, table and specified properties.
    *
    * @param url the server url
    * @param table the external database table being read
    * @param properties additional supported JDBC connection properties
    * @return DataFrame
    */
  def sqlDB(url: String, table: String, properties: Properties): DataFrame = {
    reader.jdbc(createJDBCUrl(url), table, properties)
  }

  /**
    * Creates a [[DataFrame]] based on the url, table, predicates and specified properties.
    *
    * @param url the server url.
    * @param table the external database table being read.
    * @param predicates condition in the where clause for each partition.
    * @param properties additional supported JDBC connection properties.
    * @return DataFrame
    */
  def sqlDB(url: String, table: String, predicates: Array[String], properties: Properties): DataFrame = {
    reader.jdbc(createJDBCUrl(url), table, predicates, properties)
  }

  /**
    * Creates a partitioned [[DataFrame]] based on the url, table and specified properties.
    *
    * @param url the server url
    * @param table the external database table being read
    * @param columnName name of a column, used for partitioning.
    * @param lowerBound minimum value of the field in `columnName`
    * @param upperBound maximum value of the field in `columnName`
    * @param numPartitions the number of partitions of the dataframe
    * @param properties additional supported JDBC connection properties
    * @return DataFrame
    */
  def sqlDB(url: String, table: String, columnName: String, lowerBound: Long,
            upperBound: Long, numPartitions: Int, properties: Properties): DataFrame = {
    reader.jdbc(createJDBCUrl(url), table, columnName, lowerBound, upperBound, numPartitions, properties)
  }
}
