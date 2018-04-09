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

import com.microsoft.azure.sqldb.spark.SqlDBSpark
import com.microsoft.azure.sqldb.spark.config.Config

class ConnectionUtilsSpec extends SqlDBSpark {

  "createConnectionProperties" should "return all properties in configuration in a Properties object" in {
    val url = "mssql.database.windows.net"
    val database = "MyDatabase"
    val user = "admin"
    val password = "password"
    val dbTable = "dbo.Customers"

    val config = Config(Map(
      "url"          -> url,
      "databaseName" -> database,
      "user"         -> user,
      "password"     -> password,
      "dbTable"      -> dbTable
    ))

    val controlProperties = new Properties
    controlProperties.put("url", url.toLowerCase)
    controlProperties.put("databasename", database.toLowerCase)
    controlProperties.put("user", user.toLowerCase)
    controlProperties.put("password", password.toLowerCase)
    controlProperties.put("dbtable", dbTable.toLowerCase)

    val testProperties = ConnectionUtils.createConnectionProperties(config)
    Seq(testProperties.keySet()) should contain theSameElementsAs Seq(controlProperties.keySet())
  }

  "createJDBCUrl" should "return the server url with jdbc prefix" in {
    val url = "mssql.database.windows.net"
    ConnectionUtils.createJDBCUrl(url) should be ("jdbc:sqlserver://" + url)
  }

  "getQueryCustom" should "return original query in parenthesis" in {
    val query = "SELECT * FROM MYTABLE"
    ConnectionUtils.getQueryCustom(query) should be ("(" + query + ") QueryCustom")
  }

  "getTableOrQuery" should "return appropriate table or query from a config object" in {
    val dbTable = "dbo.Customers"
    val tableConfig = Config(Map(
      "url"          -> "mssql.database.windows.net",
      "databaseName" -> "MyDatabase",
      "user"         -> "admin",
      "password"     -> "password",
      "dbTable"      -> dbTable
    ))
    ConnectionUtils.getTableOrQuery(tableConfig) should be (dbTable)

    val queryCustom = "SELECT * FROM dbo.Customers"
    val queryConfig = Config(Map(
      "url"          -> "mssql.database.windows.net",
      "databaseName" -> "MyDatabase",
      "user"         -> "admin",
      "password"     -> "password",
      "QueryCustom"  -> queryCustom
    ))
    ConnectionUtils.getTableOrQuery(queryConfig) should be (ConnectionUtils.getQueryCustom(queryCustom))
  }
}
