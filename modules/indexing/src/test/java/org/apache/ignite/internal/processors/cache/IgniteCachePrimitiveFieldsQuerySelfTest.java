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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.LinkedHashMap;
import java.util.List;
import org.junit.Test;

/**
 *
 */
public class IgniteCachePrimitiveFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        // Force BinaryMarshaller.
        cfg.setMarshaller(null);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, IndexedType> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, IndexedType> ccfg = new CacheConfiguration<>(cacheName);

        QueryEntity entity = new QueryEntity(Integer.class.getName(), IndexedType.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        // Test will pass if we use java.lang.Integer instead of int.
        fields.put("iVal", "int");

        entity.setFields(fields);

        entity.setIndexes(F.asList(
            new QueryIndex("iVal")
        ));

        ccfg.setQueryEntities(F.asList(entity));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStaticCache() throws Exception {
        checkCache(ignite(0).<Integer, IndexedType>cache(CACHE_NAME));
    }

    /**
     * @throws Exception if failed.
     */
    private void checkCache(IgniteCache<Integer, IndexedType> cache) throws Exception {
        for (int i = 0; i < 1000; i++)
            cache.put(i, new IndexedType(i));

        List<List<?>> res = cache.query(new SqlFieldsQuery("select avg(iVal) from IndexedType where iVal > ?")
            .setArgs(499)).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
    }

    /**
     *
     */
    @SuppressWarnings("unused")
    private static class IndexedType {
        /** */
        private int iVal;

        /**
         * @param iVal Value.
         */
        private IndexedType(int iVal) {
            this.iVal = iVal;
        }
    }
}
