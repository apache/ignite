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

package org.apache.ignite.cache.store.jdbc.model;

import java.sql.PreparedStatement;
import java.util.Collection;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Test JDBC POJO Store Factory With Hang WriteAll Method..
 */
public class TestJdbcPojoStoreFactoryWithHangWriteAll<K, V> extends CacheJdbcPojoStoreFactory<K, V> {
    /** */
    private static long count = 0;

    /** {@inheritDoc} */
    @Override public CacheJdbcPojoStore<K, V> create() {
        CacheJdbcPojoStore<K, V> store = new TestJdbcPojoStoreWithHangWriteAll<>();

        store.setBatchSize(getBatchSize());
        store.setDialect(getDialect());
        store.setMaximumPoolSize(getMaximumPoolSize());
        store.setMaximumWriteAttempts(getMaximumWriteAttempts());
        store.setParallelLoadCacheMinimumThreshold(getParallelLoadCacheMinimumThreshold());
        store.setTypes(getTypes());
        store.setHasher(getHasher());
        store.setTransformer(getTransformer());
        store.setSqlEscapeAll(isSqlEscapeAll());
        store.setDataSource(getDataSourceFactory().create());

        return store;
    }

    /** */
    public static class TestJdbcPojoStoreWithHangWriteAll<K,V> extends CacheJdbcPojoStore<K,V> {
        /** {@inheritDoc} */
        @Override protected void fillParameter(PreparedStatement stmt, int idx, JdbcTypeField field, @Nullable Object fieldVal) throws CacheException {
            try {
                super.fillParameter(stmt, idx, field, fieldVal);
            }
            catch (Exception e) {
                log.error("Failed to fill parameter [idx=" + idx + ", field=" + field + ", val=" + fieldVal + ']', e);

                throw e;
            }
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws CacheLoaderException {
            DataSource ds = getDataSource();

            try {
                if (ds instanceof TestJdbcPojoDataSource)
                    ((TestJdbcPojoDataSource)ds).switchPerThreadMode(false);

                super.loadCache(clo, args);
            }
            finally {
                if (ds instanceof TestJdbcPojoDataSource)
                    ((TestJdbcPojoDataSource)ds).switchPerThreadMode(true);
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            try {
                super.delete(key);
            }
            catch (Exception e) {
                log.error("Failed to delete entry from cache store: " + key, e);
            }
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            try {
                super.deleteAll(keys);
            }
            catch (Exception e) {
                log.error("Failed to delete entries from cache store: " + keys, e);
            }
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
            try {
                super.write(entry);
            }
            catch (Exception e) {
                log.error("Failed to write entry to cache store: " + entry, e);
            }

        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
            try {
                super.writeAll(entries);

                Thread.sleep(10000);

                count += entries.size();

                log.info("Count of load data: " + count);
            }
            catch (Exception e) {
                log.error("Failed to write entries to cache store: " + entries, e);
            }
        }
    }
}

