/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.jdbc.thin;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinAbstractSelfTest extends GridCommonAbstractTest {
    /**
     * @param r Runnable to check support.
     */
    protected void checkNotSupported(final RunnableX r) {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    r.run();

                    return null;
                }
            }, SQLFeatureNotSupportedException.class, null);
    }

    /**
     * @param r Runnable to check on closed connection.
     */
    protected void checkConnectionClosed(final RunnableX r) {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    r.run();

                    return null;
                }
            }, SQLException.class, "Connection is closed");
    }

    /**
     * @param r Runnable to check on closed statement.
     */
    protected void checkStatementClosed(final RunnableX r) {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    r.run();

                    return null;
                }
            }, SQLException.class, "Statement is closed");
    }

    /**
     * @param r Runnable to check on closed result set.
     */
    protected void checkResultSetClosed(final RunnableX r) {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    r.run();

                    return null;
                }
            }, SQLException.class, "Result set is closed");
    }

    /**
     * Runnable that can throw an exception.
     */
    interface RunnableX {
        /**
         * @throws Exception On error.
         */
        void run() throws Exception;
    }
}