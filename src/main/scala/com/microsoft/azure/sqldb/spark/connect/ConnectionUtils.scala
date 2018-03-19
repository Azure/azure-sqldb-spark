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

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}

/**
  * Helper and utility methods used for setting up or using a connection
  */
private[spark] object ConnectionUtils {

  /**
    * Retrieves all connection properties in the Config object
    * and returns them as a [[Properties]] object.
    *
    * @param config the Config object with specified connection properties.
    * @return A connection [[Properties]] object.
    */
  def createConnectionProperties(config: Config): Properties = {
    val connectionProperties = new Properties()
    for (key <- config.getAllKeys) {
      connectionProperties.put(key.toString, config.get[String](key.toString).get)
    }
    connectionProperties
  }

  /**
    * Adds the "jdbc:sqlserver://" suffix to a general server url
    *
    * @param url the string url without the JDBC prefix
    * @return the url with the added JDBC prefix
    */
  def createJDBCUrl(url: String): String = SqlDBConfig.JDBCUrlPrefix + url

  /**
    * Gets a JDBC connection based on Config properties
    *
    * @param config any read or write Config
    * @return a JDBC Connection
    */
  def getConnection(config: Config): Connection = {
    Class.forName(SqlDBConfig.SQLjdbcDriver)
    DriverManager.getConnection(
      createJDBCUrl(config.get[String](SqlDBConfig.URL).get), createConnectionProperties(config))
  }

  /**
    * Retrieves the DBTable or QueryCustom specified in the config.
    * NOTE: only one property can exist within config.
    *
    * @param config the Config object with specified properties.
    * @return The specified DBTable or QueryCustom
    */
  def getTableOrQuery(config: Config): String = {
    config.get[String](SqlDBConfig.DBTable).getOrElse(
      getQueryCustom(config.get[String](SqlDBConfig.QueryCustom).get)
    )
  }

  /**
    * The JDBC driver requires parentheses and a temp variable around any custom queries.
    * This adds the required syntax so users only need to specify the query.
    *
    * @param query the default query
    * @return the syntactically correct query to be executed by the JDBC driver.
    */
  def getQueryCustom(query: String): String = s"($query) QueryCustom"

}
