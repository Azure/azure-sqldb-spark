package com.microsoft.azure.sqldb.spark.bulk

import com.microsoft.azure.sqldb.spark.SqlDBSpark
import com.microsoft.azure.sqldb.spark.config.Config

class BulkCopyUtilsSpec extends SqlDBSpark {
  "getBulkCopyOptions" should "add the correct options from Config to SQLServerBulkCopyOptions" in {
    val bulkCopyBatchSize = "2500"
    val bulkCopyTimeout = "120"
    val bulkCopyCheckConstraints = "true"
    val bulkCopyFireTriggers = "true"
    val bulkCopyKeepIdentity = "true"
    val bulkCopyKeepNulls = "true"
    val bulkCopyTableLock = "true"
    val bulkCopyUseInternalTransaction = "true"
    val bulkCopyAllowEncryptedValueModifications = "true"

    val config = Config(Map(
      "url" -> "mssql.database.windows.net",
      "databaseName" -> "MyDatabase",
      "user" -> "admin@microsoft.com",
      "password" -> "password",
      "dbTable" -> "dbo.Customers",
      "authentication" -> "ActiveDirectoryPassword",
      "trustServerCertificate" -> "true",
      "encrypt" -> "true",
      "hostNameInCertificate" -> "*.database.windows.net",
      "bulkCopyBatchSize" -> bulkCopyBatchSize,
      "bulkCopyTimeout" -> bulkCopyTimeout,
      "bulkCopyCheckConstraints" -> bulkCopyCheckConstraints,
      "bulkCopyFireTriggers" -> bulkCopyFireTriggers,
      "bulkCopyKeepIdentity" -> bulkCopyKeepIdentity,
      "bulkCopyKeepNulls" -> bulkCopyKeepNulls,
      "bulkCopyTableLock" -> bulkCopyTableLock,
      "bulkCopyUseInternalTransaction" -> bulkCopyUseInternalTransaction,
      "bulkCopyAllowEncryptedValueModifications" -> bulkCopyAllowEncryptedValueModifications
    ))

    val bulkCopyOptions = BulkCopyUtils.getBulkCopyOptions(config)
    bulkCopyOptions.getBatchSize should be (bulkCopyBatchSize.toInt)
    bulkCopyOptions.getBulkCopyTimeout should be (bulkCopyTimeout.toInt)
    bulkCopyOptions.isCheckConstraints should be (bulkCopyCheckConstraints.toBoolean)
    bulkCopyOptions.isFireTriggers should be (bulkCopyFireTriggers.toBoolean)
    bulkCopyOptions.isKeepIdentity should be (bulkCopyKeepIdentity.toBoolean)
    bulkCopyOptions.isKeepNulls should be (bulkCopyKeepNulls.toBoolean)
    bulkCopyOptions.isTableLock should be (bulkCopyTableLock.toBoolean)
    bulkCopyOptions.isUseInternalTransaction should be (bulkCopyUseInternalTransaction.toBoolean)
    bulkCopyOptions.isAllowEncryptedValueModifications should be (bulkCopyAllowEncryptedValueModifications.toBoolean)
  }
}
