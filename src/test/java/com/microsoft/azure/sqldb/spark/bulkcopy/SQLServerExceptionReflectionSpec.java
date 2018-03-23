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

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class SQLServerExceptionReflectionSpec {

    @Test(expected = SQLServerException.class)
    public void throwSQLServerExceptionTest1() throws SQLServerException {
        String text = "Testing error text";
        String state = "Testing error state";
        int code = 1;
        Exception caughtException = new Exception();

        SQLServerException exception = null;
        try {
            exception = SQLServerExceptionReflection.throwSQLServerException(text, state, code, caughtException);
        } catch (Exception e){
            fail("A SQLServerException should have been successfully constructed");
        }

        assertEquals(text, exception.getMessage());
        assertEquals(state, exception.getSQLState());
        assertEquals(code, exception.getErrorCode());
        assertEquals(caughtException, exception.getCause());

        throw exception;
    }

    @Test(expected = SQLServerException.class)
    public void throwSQLServerExceptionTest2() throws SQLServerException {
        String text = "Testing error text";
        Exception caughtException = new Exception();

        SQLServerException exception = null;
        try {
            exception = SQLServerExceptionReflection.throwSQLServerException(text, caughtException);
        } catch (Exception e){
            fail("A SQLServerException should have been successfully constructed");
        }

        assertEquals(text, exception.getMessage());
        assertEquals(caughtException, exception.getCause());

        throw exception;
    }
}
