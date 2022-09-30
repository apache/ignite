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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Test query on index by search row with mismatched column type.
 */
public class IndexColumnTypeMismatchTest extends AbstractIndexingCommonTest {
    /** */
    private static final String LOG_MSG =
        "Provided value can't be used as index search bound due to column data type mismatch";

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private static final int ROW_COUNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testIndexColTypeMismatch() throws Exception {
        LogListener lsnr = LogListener.matches(LOG_MSG).times(1).build();

        listeningLog.registerListener(lsnr);

        IgniteEx ignite = startGrid(0);

        sql(ignite, "CREATE TABLE test (id INTEGER, val VARCHAR, PRIMARY KEY (id)) WITH \"CACHE_NAME=test\"");

        sql(ignite, "CREATE INDEX test_idx ON test (val)");

        for (int i = 0; i < ROW_COUNT; i++)
            sql(ignite, "INSERT INTO test VALUES (?, ?)", i, i);

        for (int i = 0; i < ROW_COUNT; i++) {
            // Use 'int' as a search row for 'string' index.
            List<List<?>> res = sql(ignite, "SELECT val FROM test WHERE val = ?", i);

            assertEquals(1, res.size());
            assertEquals(String.valueOf(i), res.get(0).get(0));
        }

        List<List<?>> res = sql(ignite, "SELECT val FROM test WHERE val < ?", 50);

        assertEquals(50, res.size());

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testIndexDifferentTypesComparable() throws Exception {
        LogListener lsnr = LogListener.matches(LOG_MSG).times(0).build();

        listeningLog.registerListener(lsnr);

        IgniteEx ignite = startGrid(0);

        sql(ignite, "CREATE TABLE test (id INTEGER, val INTEGER, PRIMARY KEY (id)) WITH \"CACHE_NAME=test\"");

        sql(ignite, "CREATE INDEX test_idx ON test (val)");

        for (int i = 0; i < ROW_COUNT; i++)
            sql(ignite, "INSERT INTO test VALUES (?, ?)", i, i);

        for (long i = 0; i < ROW_COUNT; i++) {
            // Use 'long' as a search row for 'int' index.
            List<List<?>> res = sql(ignite, "SELECT val FROM test WHERE val = ?", i);

            assertEquals(1, res.size());
            assertEquals((int)i, res.get(0).get(0));
        }

        List<List<?>> res = sql(ignite, "SELECT val FROM test WHERE val < ?", 50L);

        assertEquals(50, res.size());

        assertTrue(lsnr.check());
    }

    /** */
    private List<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
