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

package org.apache.ignite.internal.processors.query.timeout;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class DefaultQueryTimeoutThickJavaTest extends AbstractDefaultQueryTimeoutTest {
    /** Lazy. */
    private final boolean lazy;

    /** */
    public DefaultQueryTimeoutThickJavaTest() {
        this(false, false);
    }

    /** */
    protected DefaultQueryTimeoutThickJavaTest(boolean updateQuery, boolean lazy) {
        super(updateQuery);

        this.lazy = lazy;
    }

    /** {@inheritDoc} */
    @Override protected void prepareQueryExecution() throws Exception {
        super.prepareQueryExecution();

        startClientGrid(10);
    }

    /** {@inheritDoc} */
    @Override protected void executeQuery(String sql) throws Exception {
        executeQuery0(new SqlFieldsQuery(sql).setLazy(lazy));
    }

    /** {@inheritDoc} */
    @Override protected void executeQuery(String sql, int timeout) throws Exception {
        executeQuery0(new SqlFieldsQuery(sql)
            .setLazy(lazy)
            .setTimeout(timeout, TimeUnit.MILLISECONDS));
    }

    /** {@inheritDoc} */
    @Override protected void assertQueryCancelled(Callable<?> c) {
        // t0d0 check thrown exception in lazy mode
        GridTestUtils.assertThrows(log, c, Exception.class, "cancel");
    }

    /** */
    private void executeQuery0(SqlFieldsQuery qry) throws Exception {
        IgniteEx cli = grid(10);

        qry.setLazy(lazy);

        cli.context().query().querySqlFields(qry, false).getAll();
    }
}
