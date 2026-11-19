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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Conflict resolver must see values of thin client {@code putAllConflict} in binary form: the value class may be
 * absent on the destination cluster (CDC replication).
 */
public class ThinClientPutAllConflictKeepBinaryTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(TRANSACTIONAL));

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                return cls == CacheConflictResolutionManager.class ? (T)new TestConflictResolver<>() : null;
            }
        });

        return cfg;
    }

    /** */
    @Test
    public void testKeepBinary() throws Exception {
        startGrid(0);

        try (IgniteClient client = startClient(0)) {
            TcpClientCache<Object, Object> cache =
                (TcpClientCache<Object, Object>)client.cache(DEFAULT_CACHE_NAME).withKeepBinary();

            BinaryObject val = client.binary().builder("org.apache.ignite.MissingOnServer").setField("f", 1).build();

            cache.putAllConflict(F.asMap(1, new T3<>(val, new GridCacheVersion(1, 1, 1, 2), CU.EXPIRE_TIME_ETERNAL)));
        }

        TestConflictResolver<?, ?> rslvr =
            (TestConflictResolver<?, ?>)grid(0).cachex(DEFAULT_CACHE_NAME).context().conflictResolver();

        assertTrue(String.valueOf(rslvr.seenVal), rslvr.seenVal instanceof BinaryObject);
    }

    /** */
    private static class TestConflictResolver<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V>, CacheVersionConflictResolver {
        /** Value of the last resolved entry. */
        private volatile Object seenVal;

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
            CacheObjectValueContext ctx,
            GridCacheVersionedEntryEx<K1, V1> oldEntry,
            GridCacheVersionedEntryEx<K1, V1> newEntry,
            boolean atomicVerComparator
        ) {
            seenVal = newEntry.value(ctx);

            GridCacheVersionConflictContext<K1, V1> res =
                new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

            res.useNew();

            return res;
        }
    }
}
