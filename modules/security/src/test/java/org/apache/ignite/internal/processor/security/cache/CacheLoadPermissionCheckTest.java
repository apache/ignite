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

package org.apache.ignite.internal.processor.security.cache;

import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.util.lang.gridfunc.ContainsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;

/**
 * Test cache permission for Load cache.
 */
@RunWith(JUnit4.class)
public class CacheLoadPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** Entry. */
    private static T2<String, Integer> entry;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<String, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED)
                .setCacheStoreFactory(FactoryBuilder.factoryOf(new T2BasedStore())),

            new CacheConfiguration<String, Integer>()
                .setName(FORBIDDEN_CACHE)
                .setCacheMode(CacheMode.REPLICATED)
                .setCacheStoreFactory(FactoryBuilder.factoryOf(new T2BasedStore()))
        };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build());

        startGrid("client_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), true);

        super.beforeTestsStarted();
    }

    /**
     *
     */
    @Test
    public void test() {
        load(grid("server_node"));
        load(grid("client_node"));

        loadAsync(grid("server_node"));
        loadAsync(grid("client_node"));

        localLoad(grid("server_node"));

        localLoadAsync(grid("server_node"));
    }

    /**
     * @param r Runnable.
     */
    private void allowed(Runnable r) {
        assertAllowed(grid("server_node"), CACHE_NAME,
            (t) -> {
                entry = t;

                r.run();
            }
        );
    }

    /**
     * @param node Node.
     */
    private void load(Ignite node) {
        allowed(() -> node.cache(CACHE_NAME).loadCache(null));

        //todo Security Improvement Phase-2.
        // forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).loadCache(null));
    }

    /**
     * @param node Node.
     */
    private void loadAsync(Ignite node) {
        allowed(() -> node.cache(CACHE_NAME).loadCache(null));

        //todo Security Improvement Phase-2.
        // forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).loadCacheAsync(null).get());
    }

    /**
     * @param node Node.
     */
    private void localLoad(Ignite node) {
        allowed(() -> node.cache(CACHE_NAME).localLoadCache(null));

        //todo Security Improvement Phase-2.
        // forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).localLoadCache(null));
    }

    /**
     * @param node Node.
     */
    private void localLoadAsync(Ignite node) {
        allowed(() -> node.cache(CACHE_NAME).localLoadCacheAsync(null).get());

        //todo Security Improvement Phase-2.
        // forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).localLoadCacheAsync(null).get());
    }

    /**
     *
     */
    static class T2BasedStore implements CacheStore<String, Integer>, Serializable {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<String, Integer> clo,
            @Nullable Object... args) throws CacheLoaderException {
            assert entry != null;

            clo.apply(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public Integer load(String key) throws CacheLoaderException {
            return entry.get(key);
        }

        /** {@inheritDoc} */
        @Override public Map<String, Integer> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            return F.view(entry, new ContainsPredicate<>(Sets.newHashSet(keys)));
        }

        /** {@inheritDoc} */
        @Override public void write(
            Cache.Entry<? extends String, ? extends Integer> entry) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends String, ? extends Integer>> entries) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }
    }
}
