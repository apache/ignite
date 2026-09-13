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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/** */
@ParameterizedClass(name = "Engine={0}, atomicityMode={1}, cacheMode={2}, backups={3}, loadPreviousValue={4}")
@MethodSource("parameters")
public class CacheStoreTest extends AbstractBasicIntegrationTest {
    /** */
    @Parameter(0)
    String engine;

    /** */
    @Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameter(2)
    public CacheMode cacheMode;

    /** */
    @Parameter(3)
    public int backups;

    /** */
    @Parameter(4)
    public boolean loadPreviousValue;

    /** {@inheritDoc} */
    @BeforeAll
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        writeThroughEntries.clear();
        readThroughEntries.clear();
    }

    /** */
    private static final List<Object> writeThroughEntries = new CopyOnWriteArrayList<>();

    /** */
    private static final List<Object> readThroughEntries = new CopyOnWriteArrayList<>();

    /** */
    private static Stream<Arguments> parameters() {
        return cartesianProduct(
            List.of(CalciteQueryEngineConfiguration.ENGINE_NAME, IndexingQueryEngineConfiguration.ENGINE_NAME),
            List.of(CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL),
            List.of(CacheMode.PARTITIONED, CacheMode.REPLICATED),
            List.of(0, 1),
            List.of(false, true)
        ).stream().map(Arguments::of);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(engine.equals(CalciteQueryEngineConfiguration.ENGINE_NAME) ?
                    new CalciteQueryEngineConfiguration() : new IndexingQueryEngineConfiguration()));

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(backups)
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(new TestCacheStore<>(loadPreviousValue)))
            .setReadThrough(true)
            .setWriteThrough(true)
            .setLoadPreviousValue(loadPreviousValue)
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

    /** */
    @Test
    public void testCacheStoreOnDML() throws Exception {
        IgniteEx srv = startGrids(3);

        awaitPartitionMapExchange();

        sql(srv, "INSERT INTO tbl(id, val) VALUES (?, ?)", 1, "val1");
        checkWriteThrough(1);
        checkReadThrough(1);

        sql(srv, "INSERT INTO tbl(id, val) VALUES (?, ?),(?, ?)", 2, "val2", 3, "val3");
        checkWriteThrough(2, 3);
        checkReadThrough(2, 3);

        if (CalciteQueryEngineConfiguration.ENGINE_NAME.equals(engine)) {
            sql(srv, "MERGE INTO tbl dst USING (VALUES (?, ?),(?, ?)) AS src(id, val) ON dst.id = src.id " +
                "WHEN MATCHED THEN UPDATE SET dst.val = src.val " +
                "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)", 3, "val3new", 4, "val4");
            checkReadThrough(4);
        }
        else {
            sql(srv, "MERGE INTO tbl(id, val) VALUES (?, ?),(?, ?)", 3, "val3new", 4, "val4");
            checkReadThrough();
        }

        checkWriteThrough(3, 4);

        sql(srv, "UPDATE tbl SET val='newVal'");
        checkWriteThrough(1, 2, 3, 4);
        checkReadThrough();

        sql(srv, "DELETE FROM tbl WHERE id=1");
        checkWriteThrough(1);
        checkReadThrough();

        sql(srv, "INSERT INTO tbl(id, val) SELECT id+1000, val FROM tbl");
        checkWriteThrough(1002, 1003, 1004);
        checkReadThrough(1002, 1003, 1004);
    }

    /** */
    @Override protected List<List<?>> sql(IgniteEx ignite, String sql, Object... params) {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(params), true)
            .getAll();
    }

    /** */
    private void checkWriteThrough(Object... keys) {
        assertEqualsCollectionsIgnoringOrder(F.asList(keys), writeThroughEntries);

        writeThroughEntries.clear();
    }

    /** */
    private void checkReadThrough(Object... keys) {
        if (loadPreviousValue)
            assertEqualsCollectionsIgnoringOrder(F.asList(keys), readThroughEntries);
        else
            assertEquals(0, readThroughEntries.size());

        readThroughEntries.clear();
    }

    /** Test cache store. */
    private static class TestCacheStore<K, V> extends CacheStoreAdapter<K, V> implements Serializable {
        /** */
        private final boolean shouldLoadPrevVal;

        /** */
        private TestCacheStore(boolean value) {
            shouldLoadPrevVal = value;
        }

        /** {@inheritDoc} */
        @Override public V load(K key) {
            if (!shouldLoadPrevVal)
                throw new RuntimeException("CacheStore.load should not be called");

            readThroughEntries.add(key);

            return null;
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
