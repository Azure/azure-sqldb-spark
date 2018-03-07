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
package com.microsoft.azure.sqldb.spark.config

/**
  * Values and Functions for access and parse the configuration parameters
  */
object SqlDBConfig {

  //  Parameter names (from JDBC connection properties)
  val AccessToken = "accessToken"
  val ApplicationIntent = "applicationIntent"
  val ApplicationName = "applicationName"
  val Authentication = "authentication"
  val AuthenticationScheme = "authenticationScheme"
  val ColumnEncryptionSetting = "columnEncryptionSetting"
  val ConnectTimeout = "connectTimeout"
  val Database = "database"
  val DatabaseName = "databaseName"
  val DBTable = "dbTable"
  val DisableStatementPooling = "disableStatementPooling"
  val Driver = "driver"
  val EnablePrepareOnFirstPreparedStatementCall = "enablePrepareOnFirstPreparedStatementCall"
  val Encrypt = "encrypt"
  val FailoverPartner = "failoverPartner"
  val Fips = "fips"
  val FipsProvider = "fipsProvider"
  val GSSCredential = "gsscredential"
  val HostNameInCertificate = "hostNameInCertificate"
  val InstanceName = "instanceName"
  val IntegratedSecurity = "integratedSecurity"
  val JaasConfigurationName = "jaasConfigurationName"
  val KeyStoreAuthentication = "keyStoreAuthentication"
  val KeyStoreLocation = "keyStoreLocation"
  val KeyStoreSecret = "keyStoreSecret"
  val LastUpdateCount = "lastUpdateCount"
  val LockTimeout = "lockTimeout"
  val LoginTimeout = "loginTimeout"
  val MultiSubnetFailover = "multiSubnetFailover"
  val PacketSize = "packetSize"
  val Password = "password"
  val PortNumber = "portNumber"
  val Port = "port"
  val QueryTimeout = "queryTimeout"
  val ResponseBuffering = "responseBuffering"
  val SelectMethod = "selectMethod"
  val SendStringParametersAsUnicode = "sendStringParametersAsUnicode"
  val SendTimeAsDatetime = "sendTimeAsDatetime"
  val ServerName = "serverName"
  val Server = "server"
  val ServerNameAsACE = "serverNameAsACE"
  val ServerPreparedStatementDiscardThreshold = "serverPreparedStatementDiscardThreshold"
  val ServerSpn = "serverSpn"
  val SocketTimeout = "socketTimeout"
  val TransparentNetworkIPResolution = "transparentNetworkIPResolution"
  val TrustServerCertificate = "trustServerCertificate"
  val TrustStore = "trustStore"
  val TrustStorePassword = "trustStorePassword"
  val TrustStoreType = "trustStoreType"
  val URL = "url"
  val User = "user"
  val WorkstationID = "workstationID"
  val XopenStates = "xopenStates"

  // Bulk Copy API Options
  val BulkCopyBatchSize = "bulkCopyBatchSize"
  val BulkCopyTimeout = "bulkCopyTimeout"
  val BulkCopyCheckConstraints = "bulkCopyCheckConstraints"
  val BulkCopyFireTriggers = "bulkCopyFireTriggers"
  val BulkCopyKeepIdentity = "bulkCopyKeepIdentity"
  val BulkCopyKeepNulls = "bulkCopyKeepNulls"
  val BulkCopyTableLock = "bulkCopyTableLock"
  val BulkCopyUseInternalTransaction = "bulkCopyUseInternalTransaction"
  val BulkCopyAllowEncryptedValueModifications = "bulkCopyAllowEncryptedValueModifications"

  // Bulk Copy API Default Settings
  val BulkCopyBatchSizeDefault = 0
  val BulkCopyTimeoutDefault = 60
  val BulkCopyCheckConstraintsDefault = false
  val BulkCopyFireTriggersDefault = false
  val BulkCopyKeepIdentityDefault = false
  val BulkCopyKeepNullsDefault = false
  val BulkCopyTableLockDefault = false
  val BulkCopyUseInternalTransactionDefault = false
  val BulkCopyAllowEncryptedValueModificationsDefault = false

  // Extra constants
  val JDBCUrlPrefix = "jdbc:sqlserver://"
  val QueryCustom = "QueryCustom"
  val SQLjdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  // Mandatory fields to construct a Config
  val required = List(
    User,
    Password,
    DatabaseName,
    URL
  )
}
