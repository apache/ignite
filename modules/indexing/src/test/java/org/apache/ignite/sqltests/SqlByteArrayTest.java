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

package org.apache.ignite.sqltests;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.AbstractBinaryArraysTest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/** Test to check CRUD with array keys and values. */
public class SqlByteArrayTest extends AbstractBinaryArraysTest {
    /** */
    private static Ignite server;

    /** */
    private static Ignite client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrid(0);
        client = startClientGrid(1);
    }

    /** */
    @Test
    public void testSQLByteArrayValue() {
        doTest(server, byte[].class, new byte[] {1, 2}, new byte[] {1, 2, 3});
        doTest(client, byte[].class, new byte[] {1, 2}, new byte[] {1, 2, 3});
    }

    /** */
    private <T> void doTest(Ignite ignite, Class<T> cls, T val1, T val2) {
        ignite.destroyCache("array-cache");

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<Integer, Object>()
            .setName("array-cache")
            .setQueryEntities(Arrays.asList(
                new QueryEntity(Integer.class.getName(), cls.getName())
                    .setTableName("array_table")));

        IgniteCache<Integer, Object> cache = ignite.createCache(ccfg);

        awaitCacheOnClient(client, ccfg.getName());

        cache.query(new SqlFieldsQuery("INSERT INTO array_table (_key, _val) VALUES (?, ?)").setArgs(2, val1)).getAll();

        assertTrue(cache.containsKey(2));
        assertEquals(0, F.compareArrays(val1, cache.get(2)));

        cache.query(new SqlFieldsQuery("UPDATE array_table SET _val = ? WHERE _key = ? ").setArgs(val2, 2)).getAll();

        assertTrue(cache.containsKey(2));
        assertEquals(0, F.compareArrays(val2, cache.get(2)));

        cache.query(new SqlFieldsQuery("DELETE FROM array_table WHERE _key = ?").setArgs(2)).getAll();

        assertFalse(cache.containsKey(2));
    }
}
