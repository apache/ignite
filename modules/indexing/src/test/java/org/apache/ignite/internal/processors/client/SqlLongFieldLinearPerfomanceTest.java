/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Check that increasing column value in 10 times gets increasing select execution time no more then 10 times.  */
public class SqlLongFieldLinearPerfomanceTest extends GridCommonAbstractTest {
    /** Ignite instance. */
    private Ignite ignite;

    /** cache name */
    private final String cacheName = "default_cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = startGrids(1);

        createTable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        stopAllClients(true);

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return new IgniteConfiguration();
    }

    /** create table */
    private void createTable() {
        IgniteCache c = ignite.getOrCreateCache(cacheName);

        c.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, V VARCHAR)")).getAll();
    }

    private IgniteConfiguration getClientConfuguration() throws Exception {
        return new IgniteConfiguration(getConfiguration()).setClientMode(true);
    }

    /** test performance */
    @Test
    public void test() throws Exception {
        try (Ignite client = startClientGrid()) {
            int len = 400_000;

            long t1 = getTime(client, len);
            long t2 = getTime(client, len * 10);
            long t3 = getTime(client, len * 100);
            long t4 = getTime(client, len * 1000);

            assertTrue(t2 <= t1 * 10);
            assertTrue(t3 <= t1 * 100);
            assertTrue(t4 <= t1 * 1000);
        }
    }

    /** get time of select execution by string value length */
    public long getTime(Ignite client, int len) throws Exception {
            IgniteCache c = client.getOrCreateCache("default");

            c.query(new SqlFieldsQuery("DELETE FROM T1"));

            String bigValue = StringUtils.leftPad("1", len, "_");

            c.query(new SqlFieldsQuery("INSERT INTO T1(ID, V) VALUES (?, ?)").setArgs(0, bigValue));

            long d1 = System.currentTimeMillis();
            c.query(new SqlFieldsQuery("SELECT T1.ID, T1.V FROM T1")).getAll().size();
            long d2 = System.currentTimeMillis();

            return d2 - d1;
    }
}
