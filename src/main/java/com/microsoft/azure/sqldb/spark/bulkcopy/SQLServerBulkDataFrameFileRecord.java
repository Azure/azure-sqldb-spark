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

/**
 * Class to allow SQLServerBulkCopy to write data to SQL Server Tables from Spark DataFrames
 */
public class SQLServerBulkDataFrameFileRecord implements ISQLServerBulkRecord, java.lang.AutoCloseable {

    private Iterator<Row> iterator;

    private Map<Integer, ColumnMetadata> columnMetadata;

    /*
     * Logger
     */
    protected String loggerPackageName = "com.microsoft.jdbc.SQLServerBulkRecord";
    protected static java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.jdbc.SQLServerBulkRecord");

    /*
     * Contains the format that java.sql.Types.TIMESTAMP_WITH_TIMEZONE data should be read in as.
     */
    protected DateTimeFormatter dateTimeFormatter = null;

    /*
     * Contains the format that java.sql.Types.TIME_WITH_TIMEZONE data should be read in as.
     */
    protected DateTimeFormatter timeFormatter = null;

    void addColumnMetadataInternal(int positionInSource, String name, int jdbcType, int precision, int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {}

    @Override
    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        addColumnMetadataInternal(positionInSource, name, jdbcType, precision, scale, dateTimeFormatter);
    }

    @Override
    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision,
            int scale) throws SQLServerException {
        addColumnMetadataInternal(positionInSource, name, jdbcType, precision, scale, null);
    }

    public SQLServerBulkDataFrameFileRecord(Iterator<Row> iterator, BulkCopyMetadata metadata) {
        this.iterator = iterator;
        this.columnMetadata = metadata.getMetadata();
    }

    public DateTimeFormatter getDateTimeFormatter(int column) {
        return columnMetadata.get(column).getDateTimeFormatter();
    }

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        return columnMetadata.get(column).dateTimeFormatter;
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

        // Keys of the columnMetadata is a database table column with index starting from 1.
        // rowData is an array with index starting from 0.
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
            } catch (IllegalArgumentException illegalArgumentException) {
                String value = "'" + row.get(pair.getKey() - 1) + "'";
                MessageFormat form = new MessageFormat(getSQLServerExceptionErrorMsg("R_errorConvertingValue"));
                String errText = form.format(new Object[]{value, JDBCType.valueOf(cm.getColumnType()).getName()});

                try {
                    throw SQLServerExceptionReflection.throwSQLServerException(errText, null, 0, illegalArgumentException);
                } catch (Exception e) {
                    throw new IllegalArgumentException(errText, illegalArgumentException);
                }
            } catch (ArrayIndexOutOfBoundsException arrayOutOfBoundsException) {
                String errText = getSQLServerExceptionErrorMsg("R_schemaMismatch");

                try {
                    throw SQLServerExceptionReflection.throwSQLServerException(errText, arrayOutOfBoundsException);
                } catch (Exception e) {
                    throw new ArrayIndexOutOfBoundsException(errText);
                }
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

    @Override
    public void setTimestampWithTimezoneFormat(String dateTimeFormat) {
        loggerExternal.entering(loggerPackageName, "setTimestampWithTimezoneFormat", dateTimeFormat);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
        loggerExternal.exiting(loggerPackageName, "setTimestampWithTimezoneFormat");
    }


    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(loggerPackageName, "setTimestampWithTimezoneFormat",
                    new Object[] {dateTimeFormatter});
        }
        this.dateTimeFormatter = dateTimeFormatter;
        loggerExternal.exiting(loggerPackageName, "setTimestampWithTimezoneFormat");
    }

    @Override
    public void setTimeWithTimezoneFormat(String timeFormat) {
        loggerExternal.entering(loggerPackageName, "setTimeWithTimezoneFormat", timeFormat);
        this.timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
        loggerExternal.exiting(loggerPackageName, "setTimeWithTimezoneFormat");
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(loggerPackageName, "setTimeWithTimezoneFormat", new Object[] {dateTimeFormatter});
        }
        this.timeFormatter = dateTimeFormatter;
        loggerExternal.exiting(loggerPackageName, "setTimeWithTimezoneFormat");
    }

    private String getSQLServerExceptionErrorMsg(String type) {
        return SQLServerResource.getBundle("com.microsoft.sqlserver.jdbc.SQLServerResource").getString(type);
    }
}
