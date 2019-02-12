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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for query memory manager.
 */
public class IgniteH2QueryMemoryManagerSelfTest extends GridCommonAbstractTest {
    /** Row count. */
    private static final int ROW_CNT = 1000;

    /** 1M constant. */
    private static final int MAX_MEM_1M = 1024 * 1024;

    /** Lazy query execution. */
    private boolean lazy;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    private void populateData() {
        sql("create table T (id int primary key, ref_key int, name varchar)", MAX_MEM_1M);

        for (int i = 0; i < ROW_CNT; ++i)
            sql("insert into T VALUES (?, ?, ?)", MAX_MEM_1M, i, i, UUID.randomUUID().toString());
    }

    /**
     */
    @Test
    public void testLazyFalse() {
        lazy = false;

        checkQueries();
    }

    /**
     */
    @Test
    public void testLazyTrue() {
        lazy = true;

        checkQueries();
    }

    /**
     */
    private void checkQueries() {
        List<List<?>> res = sql("select * from T order by T.id", MAX_MEM_1M);

        GridTestUtils.assertThrows(log, () -> {
            sql("select * from T as T0, T as T1 order by T0.id", MAX_MEM_1M);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Check that two small queries works.
        sql("select * from T as T0, T as T1 where T0.id < 2 order by T0.id", MAX_MEM_1M);
        sql("select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4 order by T0.id", MAX_MEM_1M);

        // Check query that is mapped to two map queries.
        GridTestUtils.assertThrows(log, () -> {
            sql("(select * from T as T0, T as T1 where T0.id < 2 order by T0.id) " +
                "UNION " +
                "(select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4 order by T0.id)", MAX_MEM_1M);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Query with small local result.
        sql("select * from T order by T.id", MAX_MEM_1M);
    }

    /**
     * @param sql SQL query
     * @param maxMem Memory limit.
     * @param args Query parameters.
     * @return Results set.
     */
    private List<List<?>> sql(String sql, long maxMem, Object... args) {
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null).maxMemory(maxMem).setLazy(lazy).setArgs(args), false)
            .getAll();
    }
}
