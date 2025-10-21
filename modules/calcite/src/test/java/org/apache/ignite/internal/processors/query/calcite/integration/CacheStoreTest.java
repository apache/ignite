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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/** */
public class CacheStoreTest extends AbstractMultiEngineIntegrationTest {
    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(3)
    public int backups;

    /** */
    private static final List<Object> writeThroughEntries = new CopyOnWriteArrayList<>();

    /** */
    @Parameterized.Parameters(name = "Query engine={0}, atomicityMode={1}, cacheMode={2}, backups={3}")
    public static Collection<?> params() {
        return cartesianProduct(
            asList(CalciteQueryEngineConfiguration.ENGINE_NAME, IndexingQueryEngineConfiguration.ENGINE_NAME),
            asList(CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL),
            asList(CacheMode.PARTITIONED, CacheMode.REPLICATED),
            asList(0, 1)
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(backups)
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setCacheStoreFactory(TestCacheStore::new)
            .setReadThrough(true)
            .setWriteThrough(true)
            .setLoadPreviousValue(true)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, String.class)
                .setTableName("tbl")
                .setKeyFieldName("id")
                .setValueFieldName("val")
                .addQueryField("id", Integer.class.getName(), null)
                .addQueryField("val", String.class.getName(), null)
            )));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        writeThroughEntries.clear();
    }

    /** */
    @Test
    public void testCacheStoreOnDML() throws Exception {
        IgniteEx srv = startGrids(3);

        sql(srv, "INSERT INTO tbl(id, val) VALUES (?, ?)", 1, "val1");
        checkWriteThroughEntries(1);

        sql(srv, "INSERT INTO tbl(id, val) VALUES (?, ?),(?, ?)", 2, "val2", 3, "val3");
        checkWriteThroughEntries(2, 3);

        if (CalciteQueryEngineConfiguration.ENGINE_NAME.equals(engine)) {
            sql(srv, "MERGE INTO tbl dst USING (VALUES (?, ?),(?, ?)) AS src(id, val) ON dst.id = src.id " +
                "WHEN MATCHED THEN UPDATE SET dst.val = src.val " +
                "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)", 3, "val3new", 4, "val4");
        }
        else
            sql(srv, "MERGE INTO tbl(id, val) VALUES (?, ?),(?, ?)", 3, "val3new", 4, "val4");

        checkWriteThroughEntries(3, 4);

        sql(srv, "UPDATE tbl SET val='newVal'");
        checkWriteThroughEntries(1, 2, 3, 4);

        sql(srv, "DELETE FROM tbl WHERE id=1");
        checkWriteThroughEntries(1);

        sql(srv, "INSERT INTO tbl(id, val) SELECT id+1000, val FROM tbl");
        checkWriteThroughEntries(1002, 1003, 1004);
    }

    /** */
    private void checkWriteThroughEntries(Object... keys) {
        assertEqualsCollectionsIgnoringOrder(F.asList(keys), writeThroughEntries);

        writeThroughEntries.clear();
    }

    /** Test cache store. */
    private static class TestCacheStore<K, V> extends CacheStoreAdapter<K, V> implements Serializable {
        /** {@inheritDoc} */
        @Override public V load(K key) {
            throw new RuntimeException("CacheStore.load should not be called");
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends V> e) throws CacheWriterException {
            assertNotNull(e.getValue());

            writeThroughEntries.add(e.getKey());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object k) throws CacheWriterException {
            writeThroughEntries.add(k);
        }
    }
}
