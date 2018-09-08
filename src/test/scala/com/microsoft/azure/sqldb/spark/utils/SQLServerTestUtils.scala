package com.microsoft.azure.sqldb.spark.utils

import java.sql.{Connection, DriverManager}

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils.{createConnectionProperties, createJDBCUrl}

private[spark] class SQLServerTestUtils {
  val DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


  private def getConnection(writeConfig: Config): Connection = {
    val url = writeConfig.get[String](SqlDBConfig.URL).get
    val db = writeConfig.get[String](SqlDBConfig.DatabaseName).get
    val properties = createConnectionProperties(writeConfig)
    val user = writeConfig.get[String](SqlDBConfig.User).get
    val password = writeConfig.get[String](SqlDBConfig.Password).get
    val port = writeConfig.get[String](SqlDBConfig.PortNumber).getOrElse("1433")
    if(db.equals(null) || url.equals(null)){
      return null
    }
    Class.forName(DRIVER_NAME)
    var conn: Connection = DriverManager.getConnection(createJDBCUrl(url, Some(port))+";database="+db, user, password)
    return conn
  }

  def dropAllTables(): Unit = {
    //TODO
  }

  def createTable(config: Map[String, String], columns: Map[String, String]): Boolean ={
    var writeConfig = Config(config)
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(null)
    if(table.equals(null)) {
      return false
    }
    val conn = getConnection(writeConfig)
    if(conn.equals(null)){
      return false
    }
    if(!dataTypeVerify(columns)){
      return false
    }
    var columnDef = ""
    for(column <- columns){
      columnDef += column._1 + " " + column._2 + ", "
    }
    var sql = "CREATE TABLE " + table + "(" + columnDef.substring(0, columnDef.length-1) + ");"
    try{
      val stmt = conn.createStatement()
      stmt.executeUpdate(sql)
      conn.close()
    } catch {
      case e: Exception => return false
    }

    return true
  }

  def dropTable(config: Map[String, String]): Boolean ={
    var writeConfig = Config(config)
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(null)
    if(table.equals(null)) {
      return false
    }
    val conn = getConnection(writeConfig)
    if(conn.equals(null)){
      return false
    }

    var sql = "DROP TABLE " + table + ";"
    try{
      val stmt = conn.createStatement()
      stmt.executeUpdate(sql)
      conn.close()
    } catch {
      case e: Exception => return false
    }

    return true

  }

  def truncateTable(config: Map[String, String]): Boolean ={
    var writeConfig = Config(config)
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(null)
    if(table.equals(null)) {
      return false
    }
    val conn = getConnection(writeConfig)
    if(conn.equals(null)){
      return false
    }

    var sql = "TRUNCATE TABLE " + table + ";"
    try{
      val stmt = conn.createStatement()
      stmt.executeUpdate(sql)
      conn.close()
    } catch {
      case e: Exception => return false
    }

    return true
  }

  private def dataTypeVerify(typeMap: Map[String, String]): Boolean = {
    for (typeName <- typeMap.values) {
      val dataType = typeName.split('(')(0).trim

      dataType match {
        case "bigint" => ""
        case "binary" => ""
        case "bit" => ""
        case "char" => ""
        case "date" => ""
        case "datetime" => ""
        case "datetime2" => ""
        case "datetimeoffset" => ""
        case "decimal" => ""
        case "float" => ""
        case "image" => return false
        case "int" => ""
        case "money" => ""
        case "nchar" => ""
        case "ntext" => ""
        case "numeric" => ""
        case "nvarchar" => ""
        case "nvarchar(max)" => ""
        case "real" => ""
        case "smalldatetime" => ""
        case "smallint" => ""
        case "smallmoney" => ""
        case "text" => ""
        case "time" => ""
        case "timestamp" => ""
        case "tinyint" => ""
        case "udt" => return false
        case "uniqueidentifier" => ""
        case "varbinary" => ""
        //case "varbinary(max)" => ""
        case "varchar" => ""
        //case "varchar(max)" => ""
        case "xml" => ""
        case "sqlvariant" => ""
        case "geometry" => ""
        case "geography" => ""
        case _ => return false
      }
    }
    return true
  }
}
