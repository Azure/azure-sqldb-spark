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

public class BulkCopyMetadataTest {

    private BulkCopyMetadata bulkCopyMetadata;

    @Before
    public void beforeEach() {
        bulkCopyMetadata = new BulkCopyMetadata();
    }

    @Test
    public void constructorTest(){
        assertEquals(0, bulkCopyMetadata.getMetadata().size());
    }

    @Test
    public void addColumnMetadataTest() {
        String columnName = "testColumn";
        int jdbcType = Types.DOUBLE;
        int precision = 15;
        int scale = 5;
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        bulkCopyMetadata.addColumnMetadata(1, columnName, jdbcType, precision, scale);
        assertEquals(1, bulkCopyMetadata.getMetadata().size());

        bulkCopyMetadata.addColumnMetadata(2, columnName, jdbcType, precision, scale, dateTimeFormatter);
        assertEquals(2, bulkCopyMetadata.getMetadata().size());
    }
}
