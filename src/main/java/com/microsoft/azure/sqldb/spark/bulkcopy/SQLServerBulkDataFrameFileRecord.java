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

import com.microsoft.azure.sqldb.spark.Logging;
import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResource;
import org.apache.spark.sql.Row;
import scala.collection.Iterator;

import java.sql.JDBCType;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

public class SQLServerBulkDataFrameFileRecord extends Logging implements ISQLServerBulkRecord, java.lang.AutoCloseable {

    private Iterator<Row> iterator;

    private Map<Integer, ColumnMetadata> columnMetadata;

    public SQLServerBulkDataFrameFileRecord(Iterator<Row> iterator, BulkCopyMetadata metadata) {
        this.iterator = iterator;
        this.columnMetadata = metadata.getMetadata();
    }

    public DateTimeFormatter getDateTimeFormatter(int column) {
        return columnMetadata.get(column).getDateTimeFormatter();
    }

    @Override
    public void close() throws SQLServerException {
        // nothing to close
    }

    @Override
    public String getColumnName(int column) {
        return columnMetadata.get(column).getColumnName();
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnMetadata.keySet();
    }

    @Override
    public int getColumnType(int column) {
        return columnMetadata.get(column).getColumnType();
    }

    @Override
    public int getPrecision(int column) {
        return columnMetadata.get(column).getPrecision();
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        Row row = iterator.next();
        Object[] rowData = new Object[row.length()];

        for (Map.Entry<Integer, ColumnMetadata> pair : columnMetadata.entrySet()) {
            ColumnMetadata cm  = pair.getValue();

            try {
                switch (cm.getColumnType()){
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.TIMESTAMP_WITH_TIMEZONE: {
                        OffsetTime offsetTimeValue;

                        if (cm.getDateTimeFormatter() != null)
                            offsetTimeValue = OffsetTime.parse(row.get(pair.getKey() - 1).toString(), cm.getDateTimeFormatter());
                        else
                            offsetTimeValue = OffsetTime.parse(row.get(pair.getKey() - 1).toString());

                        rowData[pair.getKey() - 1] = offsetTimeValue;
                        break;
                    }

                    case Types.NULL: {
                        rowData[pair.getKey() - 1] = null;
                        break;
                    }

                    default: {
                        rowData[pair.getKey() - 1] = row.get(pair.getKey() - 1);
                        break;
                    }
                }
            } catch (IllegalArgumentException exception) {
                String value = "'" + row.get(pair.getKey() - 1) + "'";
                MessageFormat form = new MessageFormat(getSQLServerExceptionErrorMsg("R_errorConvertingValue"));

                throw new SQLServerException(form.format(new Object[]{value, JDBCType.valueOf(cm.getColumnType()).getName()}), null, 0, exception);
            } catch (ArrayIndexOutOfBoundsException exception) {
                throw new SQLServerException(getSQLServerExceptionErrorMsg("R_schemaMismatch"), exception);
            }
        }

        return rowData;
    }

    @Override
    public int getScale(int column) {
        return columnMetadata.get(column).getScale();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean next() throws SQLServerException {
        return iterator.hasNext();
    }

    private String getSQLServerExceptionErrorMsg(String type) {
        return SQLServerResource.getBundle("com.microsoft.sqlserver.jdbc.SQLServerResource").getString(type);
    }
}
