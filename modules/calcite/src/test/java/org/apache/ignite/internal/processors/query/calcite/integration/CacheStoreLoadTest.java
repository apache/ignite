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

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/** */
public class CacheStoreLoadTest extends AbstractMultiEngineIntegrationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheStoreFactory(TestCacheStore::new)
            .setReadThrough(true)
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
    }

    /** */
    @Test
    public void testSingleInsertSkipCacheStoreLoad() throws Exception {
        IgniteEx srv = startGrid(0);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO tbl(id, val) VALUES(?, ?)");

        qry.setArgs(1, "val1");

        srv.cache(DEFAULT_CACHE_NAME).query(qry);
    }

    /** Test cache store. */
    private static class TestCacheStore<K, V> extends CacheStoreAdapter<K, V> {
        /** {@inheritDoc} */
        @Override public V load(K key) {
            throw new RuntimeException("CacheStore.load should not be called");
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
