/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for the case when client is started after the cache is already created.
 */
@RunWith(JUnit4.class)
public class CacheQueryNewClientSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFromNewClient() throws Exception {
        Ignite srv = startGrid("server");

        for (int iter = 0; iter < 2; iter++) {
            log.info("Iteration: " + iter);

            IgniteCache<Integer, Integer> cache1 = srv.createCache(new CacheConfiguration<Integer, Integer>().
                setName("cache1").setIndexedTypes(Integer.class, Integer.class));
            IgniteCache<Integer, Integer> cache2 = srv.createCache(new CacheConfiguration<Integer, Integer>().
                setName("cache2").setIndexedTypes(Integer.class, Integer.class));

            for (int i = 0; i < 10; i++) {
                cache1.put(i, i);
                cache2.put(i, i);
            }

            Ignition.setClientMode(true);

            Ignite client = (iter == 0) ? startGrid("client") : grid("client");

            IgniteCache<Integer, Integer> cache = client.cache("cache1");

            List<List<?>> res = cache.query(new SqlFieldsQuery(
                "select i1._val, i2._val from Integer i1 cross join \"cache2\".Integer i2")).getAll();

            assertEquals(100, res.size());

            srv.destroyCache(cache1.getName());
            srv.destroyCache(cache2.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFromNewClientCustomSchemaName() throws Exception {
        Ignite srv = startGrid("server");

        IgniteCache<Integer, Integer> cache1 = srv.createCache(new CacheConfiguration<Integer, Integer>().
            setName("cache1").setSqlSchema("cache1_sql").setIndexedTypes(Integer.class, Integer.class));
        IgniteCache<Integer, Integer> cache2 = srv.createCache(new CacheConfiguration<Integer, Integer>().
            setName("cache2").setSqlSchema("cache2_sql").setIndexedTypes(Integer.class, Integer.class));

        for (int i = 0; i < 10; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        Ignition.setClientMode(true);

        Ignite client = startGrid("client");

        IgniteCache<Integer, Integer> cache = client.cache("cache1");

        List<List<?>> res = cache.query(new SqlFieldsQuery(
            "select i1._val, i2._val from Integer i1 cross join cache2_sql.Integer i2")).getAll();

        assertEquals(100, res.size());
    }
}
