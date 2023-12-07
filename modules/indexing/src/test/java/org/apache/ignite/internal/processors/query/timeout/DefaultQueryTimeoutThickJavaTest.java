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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class DefaultQueryTimeoutThickJavaTest extends AbstractDefaultQueryTimeoutTest {
    /** Lazy mode. */
    @Parameterized.Parameter(value = 0)
    public boolean lazy;

    /** Execute update queries. */
    @Parameterized.Parameter(value = 1)
    public boolean update;

    /** Execute local queries. */
    @Parameterized.Parameter(value = 2)
    public boolean local;

    /** */
    @Parameterized.Parameters(name = "lazy={0}, update={1}, local={2}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        boolean[] arrBool = new boolean[] {true, false};

        for (boolean lazy0 : arrBool) {
            for (boolean update0 : arrBool) {
                for (boolean local0 : arrBool) {
                    if (local0 && update0)
                        continue;

                    params.add(new Object[] {lazy0, update0, local0});
                }
            }
        }

        return params;
    }

        /** {@inheritDoc} */
    @Override protected void prepareQueryExecution() throws Exception {
        super.prepareQueryExecution();

        startClientGrid("cli");
    }

    /** {@inheritDoc} */
    @Override protected void executeQuery(String sql) throws Exception {
        executeQuery0(new SqlFieldsQuery(sql));
    }

    /** {@inheritDoc} */
    @Override protected void executeQuery(String sql, int timeout) throws Exception {
        executeQuery0(new SqlFieldsQuery(sql)
            .setTimeout(timeout, TimeUnit.MILLISECONDS));
    }

    /** {@inheritDoc} */
    @Override protected void assertQueryCancelled(Callable<?> c) {
        // t0d0 check thrown exception in lazy mode
        GridTestUtils.assertThrows(log, c, Exception.class, "cancel");
    }

    /** */
    @Override protected boolean updateQuery() {
        return update;
    }

    /** */
    private void executeQuery0(SqlFieldsQuery qry) throws Exception {
        qry.setLazy(lazy);
        qry.setLocal(local);

        IgniteEx ign = local ? grid(0) : grid("cli");

        ign.context().query().querySqlFields(qry, false).getAll();
    }
}
