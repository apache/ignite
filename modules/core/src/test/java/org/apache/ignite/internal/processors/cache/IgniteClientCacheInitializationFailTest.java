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

package org.apache.ignite.internal.processors.cache;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test checks whether cache initialization error on client side
 * doesn't causes hangs and doesn't impact other caches.
 */
public class IgniteClientCacheInitializationFailTest extends GridCommonAbstractTest {
    /** Failed cache name. */
    private static final String CACHE_NAME = "cache";

    /** Atomic cache name. */
    private static final String ATOMIC_CACHE_NAME = "atomic-cache";

    /** Tx cache name. */
    private static final String TX_CACHE_NAME = "tx-cache";

    /** Near atomic cache name. */
    private static final String NEAR_ATOMIC_CACHE_NAME = "near-atomic-cache";

    /** Near tx cache name. */
    private static final String NEAR_TX_CACHE_NAME = "near-tx-cache";

    /** Failed caches. */
    private static final Set<String> FAILED_CACHES;

    static {
        Set<String> set = new HashSet<>();

        set.add(ATOMIC_CACHE_NAME);
        set.add(TX_CACHE_NAME);
        set.add(NEAR_ATOMIC_CACHE_NAME);
        set.add(NEAR_TX_CACHE_NAME);

        FAILED_CACHES = Collections.unmodifiableSet(set);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server");
        startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("server")) {
            CacheConfiguration<Integer, String> ccfg1 = new CacheConfiguration<>();

            ccfg1.setIndexedTypes(Integer.class, String.class);
            ccfg1.setName(ATOMIC_CACHE_NAME);
            ccfg1.setAtomicityMode(CacheAtomicityMode.ATOMIC);

            CacheConfiguration<Integer, String> ccfg2 = new CacheConfiguration<>();

            ccfg2.setIndexedTypes(Integer.class, String.class);
            ccfg2.setName(TX_CACHE_NAME);
            ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            cfg.setCacheConfiguration(ccfg1, ccfg2);
        }
        else {
            GridQueryProcessor.idxCls = FailedIndexing.class;

            cfg.setClientMode(true);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicCacheInitialization() throws Exception {
        checkCacheInitialization(ATOMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalCacheInitialization() throws Exception {
        checkCacheInitialization(TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_ATOMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_TX_CACHE_NAME);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkCacheInitialization(final String cacheName) throws Exception {
        Ignite client = grid("client");

        checkFailedCache(client, cacheName);

        checkFineCache(client, CACHE_NAME + 1);

        assertNull(((IgniteKernal)client).context().cache().cache(cacheName));

        checkFineCache(client, CACHE_NAME + 2);
    }

    /**
     * @param client Client.
     * @param cacheName Cache name.
     */
    private void checkFineCache(Ignite client, String cacheName) {
        IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheName);

        cache.put(1, "1");

        assertEquals("1", cache.get(1));
    }

    /**
     * @param client Client.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkFailedCache(final Ignite client, final String cacheName) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache;

                // Start cache with near enabled.
                if (NEAR_ATOMIC_CACHE_NAME.equals(cacheName) || NEAR_TX_CACHE_NAME.equals(cacheName)) {
                    CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>(cacheName)
                        .setNearConfiguration(new NearCacheConfiguration<Integer, String>());

                    if (NEAR_TX_CACHE_NAME.equals(cacheName))
                        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

                    cache = client.getOrCreateCache(ccfg);
                }
                else
                    cache = client.cache(cacheName);

                cache.put(1, "1");

                assertEquals("1", cache.get(1));

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * To fail on cache start.
     */
    private static class FailedIndexing implements GridQueryIndexing {
        /** {@inheritDoc} */
        @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public <K, V> QueryCursor<Cache.Entry<K, V>> queryDistributedSql(String schemaName, SqlQuery qry,
            boolean keepBinary, int mainCacheId) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public List<FieldsQueryCursor<List<?>>> queryDistributedSqlFields(String schemaName, SqlFieldsQuery qry,
            boolean keepBinary, GridQueryCancel cancel,
            @Nullable Integer mainCacheId, boolean failOnMultipleStmts) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long streamUpdateQuery(String spaceName, String qry, @Nullable Object[] params,
            IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public <K, V> QueryCursor<Cache.Entry<K, V>> queryLocalSql(String schemaName, SqlQuery qry,
            IndexingQueryFilter filter, boolean keepBinary) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FieldsQueryCursor<List<?>> queryLocalSqlFields(String schemaName, SqlFieldsQuery qry,
            boolean keepBinary, IndexingQueryFilter filter, GridQueryCancel cancel) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String spaceName, String qry,
            String typeName, IndexingQueryFilter filter) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void dynamicIndexCreate(String spaceName, String tblName, QueryIndexDescriptorImpl idxDesc,
            boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void dynamicIndexDrop(String spaceName, String idxName,
            boolean ifExists) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols,
                                               boolean ifTblExists, boolean ifColNotExists)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void registerCache(String cacheName, String schemaName,
            GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
            if (FAILED_CACHES.contains(cctx.name()) && cctx.kernalContext().clientNode())
                throw new IgniteCheckedException("Test query exception " + cctx.name() + " " + new Random().nextInt());
        }

        /** {@inheritDoc} */
        @Override public void unregisterCache(String spaceName, boolean destroy) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public boolean registerType(GridCacheContext cctx,
            GridQueryTypeDescriptor desc) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void store(String cacheName, GridQueryTypeDescriptor type, KeyCacheObject key, int partId,
            CacheObject val, GridCacheVersion ver, long expirationTime, long link) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void remove(String spaceName, GridQueryTypeDescriptor type, KeyCacheObject key, int partId,
            CacheObject val, GridCacheVersion ver) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void rebuildIndexesFromHash(String cacheName) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void markForRebuildFromHash(String cacheName) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public IndexingQueryFilter backupFilter(AffinityTopologyVersion topVer, int[] parts) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareNativeStatement(String space, String sql) throws SQLException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridRunningQueryInfo> runningQueries(long duration) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void cancelQueries(Collection<Long> queries) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void cancelAllQueries() {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public String schema(String cacheName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isInsertStatement(PreparedStatement nativeStmt) {
            return false;
        }
    }
}
