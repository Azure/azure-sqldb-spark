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
import java.lang.reflect.Constructor;

/**
 * ISQLServerBulkRecord requires some inherited methods to throw SQLServerException.
 * Prior to MS SQL JDBC v6.4, the SQLServerException class was only package accessible.
 * This class uses reflection in order to access SQLServerException for earlier versions of the JDBC driver.
 */
public class SQLServerExceptionReflection{

    public static SQLServerException throwSQLServerException(String errText, Throwable clause) throws Exception {
        Constructor<SQLServerException> constructor
                = SQLServerException.class.getDeclaredConstructor(String.class, Throwable.class);
        constructor.setAccessible(true);
        return constructor.newInstance(errText, clause);
    }

    public static SQLServerException throwSQLServerException(
            String errText,
            String errState,
            int errNum,
            Throwable clause) throws Exception {

        Constructor<SQLServerException> constructor
                = SQLServerException.class.getDeclaredConstructor(String.class, String.class, int.class, Throwable.class);
        constructor.setAccessible(true);
        return constructor.newInstance(errText, errState, errNum, clause);
    }
}
