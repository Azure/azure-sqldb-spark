package com.microsoft.azure.sqldb.spark.utils

import java.sql.{Connection, DriverManager}

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils.{createConnectionProperties, createJDBCUrl}


private[spark] class SQLServerTestUtils {
  val DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


  private def getConnection(writeConfig: Config, connectToDB: Boolean): Connection = {
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
    var dbConnect = ""
    if(connectToDB){
      dbConnect = ";database="+db
    }
    var conn: Connection = DriverManager.getConnection(createJDBCUrl(url, Some(port))+ dbConnect, user, password)
    return conn
  }

  private def preFlightChecks(config: Map[String, String]): Boolean = {
    var writeConfig = Config(config)
    val conn = getConnection(writeConfig, false)
    val dbName = writeConfig.get[String](SqlDBConfig.DatabaseName).get
    if(conn.equals(null)){
      return false
    }

    //Check if Server and Database Exist
    var sql = "SELECT name from master.dbo.sysdatabases where name='"+ dbName + "' OR [name]='"+ dbName + "'"
    try{
      val stmt = conn.createStatement()
      val req = stmt.executeQuery(sql)
      conn.close()
      var res = req.getString(1)
      if(res.eq(null) || res.eq("") || !res.eq(dbName)){
        return false
      }
      return true
    } catch {
      case e: Exception => return false
    }
  }

  def createTable(config: Map[String, String], columns: Map[String, String]): Boolean ={
    var writeConfig = Config(config)
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(null)
    if(table.equals(null)) {
      return false
    }
    val conn = getConnection(writeConfig, true)
    if(conn.equals(null)){
      return false
    }
    if(!dataTypeVerify(columns)){
      return false
    }

    var tableDroppedIfExists = dropTable(config)
    if(tableDroppedIfExists){
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
    } else {
      return false
    }

    return true
  }

  def dropTable(config: Map[String, String]): Boolean ={
    var writeConfig = Config(config)
    val table = writeConfig.get[String](SqlDBConfig.DBTable).getOrElse(null)
    if(table.equals(null)) {
      return false
    }
    val conn = getConnection(writeConfig, true)
    if(conn.equals(null)){
      return false
    }

    // var sql = "IF EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + table + "') DROP TABLE " + table + ";"
    var sql = "DROP TABLE IF EXISTS " + table   //Supported SQL Server 2016 and beyond
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
    val conn = getConnection(writeConfig, true)
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