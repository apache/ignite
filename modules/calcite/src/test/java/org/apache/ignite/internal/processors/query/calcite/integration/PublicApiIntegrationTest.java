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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.junit.Test;

/** Public api integration tests. */
public class PublicApiIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));

        return cfg;
    }

    /** */
    @Test
    public void testSimpleInsert() {
        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE emp(empid INTEGER, deptid INTEGER, name VARCHAR, salary INTEGER, " +
            "PRIMARY KEY(empid, deptid)) WITH \"AFFINITY_KEY=deptid\""));

        for (int i = 0; i < nodeCount() * 10; i++) {
            cache.query(new SqlFieldsQuery("INSERT INTO emp (empid, deptid, name, salary) VALUES (?, ?, ?, ?)").setArgs(
                i, i % 2, "Employee " + i, i / 10));
        }

        cache = cache.withKeepBinary();

        for (int i = nodeCount() * 10; i < 2 * nodeCount() * 10; i++) {
            cache.query(new SqlFieldsQuery("INSERT INTO emp (empid, deptid, name, salary) VALUES (?, ?, ?, ?)").setArgs(
                i, i % 2, "Employee " + i, i / 10));
        }

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM emp")).getAll();

        assertEquals("Unexpected result set size: " + res.size(), 2 * nodeCount() * 10, res.size());
    }
}
