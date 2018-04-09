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
package com.microsoft.azure.sqldb.spark

import scala.language.implicitConversions
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._

/**
  * Implicit functions added to DataFrameReader, DataFrameWriter and DataFrame objects
  */
package object connect {

  /**
    * :: DeveloperApi ::
    *
    * Helper to implicitly add SQL DB based functions to a DataFrameReader
    *
    * @param reader the DataFrameReader
    * @return the SQL DB based DataFrameReader
    */
  @DeveloperApi
  implicit def toDataFrameReaderFunctions(reader: DataFrameReader): DataFrameReaderFunctions =
    DataFrameReaderFunctions(reader)

  /**
    * :: DeveloperApi ::
    *
    * Helper to implicitly add SQL DB based functions to a DataFrameWriter
    *
    * @param writer the DataFrameWriter
    * @return the SQL DB based DataFrameWriter
    */
  @DeveloperApi
  implicit def toDataFrameWriterFunctions(writer: DataFrameWriter[_]): DataFrameWriterFunctions =
    DataFrameWriterFunctions(writer)

  /**
    * :: DeveloperApi ::
    *
    * Helper to implicitly add SQL DB based functions to a DataFrame
    *
    * @param ds the dataframe/dataset
    * @return the SQL DB based DataFrame
    */
  @DeveloperApi
  implicit def toDataFrameFunctions[T](ds: Dataset[T]): DataFrameFunctions[Row] = DataFrameFunctions[Row](ds.toDF())

}