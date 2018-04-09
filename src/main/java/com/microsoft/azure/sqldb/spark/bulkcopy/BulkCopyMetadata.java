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
package com.microsoft.azure.sqldb.spark.bulkcopy;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Public class for users to add column metadata manually
 */
public class BulkCopyMetadata implements Serializable {

    private Map<Integer, ColumnMetadata> metadata;

    public BulkCopyMetadata() {
        this.metadata = new HashMap<>();
    }

    public void addColumnMetadata(
            int column,
            String name,
            int jdbcType,
            int precision,
            int scale) {
        addColumnMetadataInternal(column, name, jdbcType, precision, scale, null);
    }

    public void addColumnMetadata(
            int column,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) {
        addColumnMetadataInternal(column, name, jdbcType, precision, scale, dateTimeFormatter);
    }

    Map<Integer, ColumnMetadata> getMetadata() {
        return metadata;
    }

    private void addColumnMetadataInternal(
            int column,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) {

        switch (jdbcType) {
            /*
             * SQL Server supports numerous string literal formats for temporal types, hence sending them as varchar with approximate
             * precision(length) needed to send supported string literals. string literal formats supported by temporal types are available in MSDN
             * page on data types.
             */
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case microsoft.sql.Types.DATETIMEOFFSET:
                // The precision is just a number long enough to hold all types of temporal data, doesn't need to be exact.
                metadata.put(column, new ColumnMetadata(name, jdbcType, 50, scale, dateTimeFormatter));
                break;

            // Redirect SQLXML as LONGNVARCHAR, SQLXML is not valid type in TDS
            case java.sql.Types.SQLXML:
                metadata.put(column, new ColumnMetadata(name, java.sql.Types.LONGNVARCHAR, precision, scale, dateTimeFormatter));
                break;

            // Redirecting Float as Double based on data type mapping
            case java.sql.Types.FLOAT:
                metadata.put(column, new ColumnMetadata(name, java.sql.Types.DOUBLE, precision, scale, dateTimeFormatter));
                break;

            // Redirecting BOOLEAN as BIT
            case java.sql.Types.BOOLEAN:
                metadata.put(column, new ColumnMetadata(name, java.sql.Types.BIT, precision, scale, dateTimeFormatter));
                break;

            default:
                metadata.put(column, new ColumnMetadata(name, jdbcType, precision, scale, dateTimeFormatter));
        }
    }
}
