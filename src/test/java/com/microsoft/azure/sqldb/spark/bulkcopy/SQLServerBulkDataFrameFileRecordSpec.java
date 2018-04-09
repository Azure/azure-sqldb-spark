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

import org.junit.Before;
import org.junit.Test;

import java.sql.Types;
import java.time.format.DateTimeFormatter;

import static junit.framework.Assert.assertEquals;

public class SQLServerBulkDataFrameFileRecordSpec {

    private SQLServerBulkDataFrameFileRecord fileRecord;

    @Before
    public void beforeEach() {
        BulkCopyMetadata bulkCopyMetadata = new BulkCopyMetadata();
        bulkCopyMetadata.addColumnMetadata(1, "Column1", Types.NVARCHAR, 128, 0);
        bulkCopyMetadata.addColumnMetadata(2, "Column2", Types.DOUBLE, 20, 10);
        bulkCopyMetadata.addColumnMetadata(3, "Column3", Types.VARCHAR, 256, 0);
        bulkCopyMetadata.addColumnMetadata(4, "Column4", Types.DATE, 50, 0, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

        fileRecord = new SQLServerBulkDataFrameFileRecord(null, bulkCopyMetadata);
    }

    @Test
    public void getColumnNameTest() {
        assertEquals("Column1", fileRecord.getColumnName(1));
        assertEquals("Column2", fileRecord.getColumnName(2));
        assertEquals("Column3", fileRecord.getColumnName(3));
        assertEquals("Column4", fileRecord.getColumnName(4));
    }

    @Test
    public void getColumnOrdinalsTest() {
        assertEquals(4, fileRecord.getColumnOrdinals().size());
    }

    @Test
    public void getColumnTypeTest() {
        assertEquals(Types.NVARCHAR, fileRecord.getColumnType(1));
        assertEquals(Types.DOUBLE, fileRecord.getColumnType(2));
        assertEquals(Types.VARCHAR, fileRecord.getColumnType(3));
        assertEquals(Types.DATE, fileRecord.getColumnType(4));
    }

    @Test
    public void getPrecisionTest() {
        assertEquals(128, fileRecord.getPrecision(1));
        assertEquals(20, fileRecord.getPrecision(2));
        assertEquals(256, fileRecord.getPrecision(3));
        assertEquals(50, fileRecord.getPrecision(4));
    }

    @Test
    public void getScaleTest() {
        assertEquals(0, fileRecord.getScale(1));
        assertEquals(10, fileRecord.getScale(2));
        assertEquals(0, fileRecord.getScale(3));
        assertEquals(0, fileRecord.getScale(4));
    }

    @Test
    public void isAutoIncrementTest() {
        assertEquals(false, fileRecord.isAutoIncrement(0));
    }
}
