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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for lazy query execution on reducer in case merge table is used.
 */
public class IgniteLazyReducerTest extends GridCommonAbstractTest {
    /** Row count. */
    private static final int ROW_CNT = 1000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception On fail.
     */
    public void testSingelNode() throws Exception {
        startGrid(0);

        populateData();

        doTest();
    }

    /**
     * @throws Exception On fail.
     */
    public void testMultipleNodes() throws Exception {
        startGrids(3);

        populateData();

        doTest();
    }

    /**
     *
     */
    private void populateData() {
        sql("create table T (id int primary key, ref_key int, name varchar)");

        for (int i = 0; i < ROW_CNT; ++i)
            sql("insert into T VALUES (?, ?, ?)", i, i, UUID.randomUUID().toString());
    }

    /**
     * Test local query execution.
     */
    private void doTest() {
        List<List<?>> res = sql("select * from T order by T.id");
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     * @return Results set.
     */
    private List<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null).setArgs(args), false).getAll();
    }
}
