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

package org.apache.ignite.internal.sql;

import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;

/**
 * Common class for SQL parser tests.
 */
@SuppressWarnings("ThrowableNotThrown")
public abstract class SqlParserAbstractSelfTest extends GridCommonAbstractTest {
    /**
     * Make sure that parse error occurs.
     *
     * @param schema Schema.
     * @param sql SQL.
     * @param msg Expected error message.
     */
    protected static void assertParseError(final String schema, final String sql, String msg) {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                new SqlParser(schema, sql).nextCommand();

                return null;
            }
        }, SqlParseException.class, msg);
    }
}
