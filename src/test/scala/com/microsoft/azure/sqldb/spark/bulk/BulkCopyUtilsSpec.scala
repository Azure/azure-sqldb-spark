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
