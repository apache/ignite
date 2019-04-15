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
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

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

    /** Mvcc tx cache name. */
    private static final String MVCC_TX_CACHE_NAME = "mvcc-tx-cache";

    /** Near atomic cache name. */
    private static final String NEAR_ATOMIC_CACHE_NAME = "near-atomic-cache";

    /** Near tx cache name. */
    private static final String NEAR_TX_CACHE_NAME = "near-tx-cache";

    /** Near mvcc tx cache name. */
    private static final String NEAR_MVCC_TX_CACHE_NAME = "near-mvcc-tx-cache";

    /** Failed caches. */
    private static final Set<String> FAILED_CACHES;

    static {
        Set<String> set = new HashSet<>();

        set.add(ATOMIC_CACHE_NAME);
        set.add(TX_CACHE_NAME);
        set.add(NEAR_ATOMIC_CACHE_NAME);
        set.add(NEAR_TX_CACHE_NAME);
        set.add(MVCC_TX_CACHE_NAME);
        set.add(NEAR_MVCC_TX_CACHE_NAME);

        FAILED_CACHES = Collections.unmodifiableSet(set);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server");
        startGrid("client");
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

            CacheConfiguration<Integer, String> ccfg3 = new CacheConfiguration<>();

            ccfg3.setIndexedTypes(Integer.class, String.class);
            ccfg3.setName(MVCC_TX_CACHE_NAME);
            ccfg3.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

            cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);
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
    @Test
    public void testAtomicCacheInitialization() throws Exception {
        checkCacheInitialization(ATOMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionalCacheInitialization() throws Exception {
        checkCacheInitialization(TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTransactionalCacheInitialization() throws Exception {
        checkCacheInitialization(MVCC_TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_ATOMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionalNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testMvccTransactionalNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_MVCC_TX_CACHE_NAME);
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
    @SuppressWarnings({"ThrowableNotThrown"})
    private void checkFailedCache(final Ignite client, final String cacheName) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache;

                // Start cache with near enabled.
                if (NEAR_ATOMIC_CACHE_NAME.equals(cacheName) || NEAR_TX_CACHE_NAME.equals(cacheName) ||
                    NEAR_MVCC_TX_CACHE_NAME.equals(cacheName)) {
                    CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>(cacheName)
                        .setNearConfiguration(new NearCacheConfiguration<Integer, String>());

                    if (NEAR_TX_CACHE_NAME.equals(cacheName))
                        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                    else if (NEAR_MVCC_TX_CACHE_NAME.equals(cacheName))
                        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

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
        @Override public void onClientDisconnect() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public SqlFieldsQuery generateFieldsQuery(String cacheName, SqlQuery qry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(
            String schemaName,
            SqlFieldsQuery qry,
            SqlClientContext cliCtx,
            boolean keepBinary,
            boolean failOnMultipleStmts,
            GridQueryCancel cancel
        ) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
            SqlClientContext cliCtx) throws IgniteCheckedException {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public long streamUpdateQuery(String schemaName, String qry, @Nullable Object[] params,
            IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String spaceName,
            String cacheName, String qry, String typeName, IndexingQueryFilter filter) throws IgniteCheckedException {
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
        @Override public void dynamicDropColumn(String schemaName, String tblName, List<String> cols,
            boolean ifTblExists, boolean ifColExists) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void registerCache(String cacheName, String schemaName,
            GridCacheContextInfo<?, ?> cacheInfo) throws IgniteCheckedException {
            if (FAILED_CACHES.contains(cacheInfo.name()) && cacheInfo.cacheContext().kernalContext().clientNode())
                throw new IgniteCheckedException("Test query exception " + cacheInfo.name() + " " + new Random().nextInt());
        }

        /** {@inheritDoc} */
        @Override public void unregisterCache(GridCacheContextInfo cacheInfo,
            boolean rmvIdx) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public UpdateSourceIterator<?> executeUpdateOnDataNodeTransactional(GridCacheContext<?, ?> cctx, int[] ids, int[] parts,
            String schema, String qry, Object[] params, int flags, int pageSize, int timeout,
            AffinityTopologyVersion topVer,
            MvccSnapshot mvccVer, GridQueryCancel cancel) throws IgniteCheckedException {
            return null;
        }

        @Override public boolean registerType(GridCacheContextInfo cacheInfo,
            GridQueryTypeDescriptor desc, boolean isSql) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void store(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row,
            CacheDataRow prevRow, boolean prevRowAvailable) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
            return null;
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
        @Override public void onKernalStop() {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public String schema(String cacheName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void checkStatementStreamable(PreparedStatement nativeStmt) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public GridQueryRowCacheCleaner rowCacheCleaner(int cacheGroupId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheContextInfo registeredCacheInfo(String cacheName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean initCacheContext(GridCacheContext ctx) throws IgniteCheckedException {
            if (FAILED_CACHES.contains(ctx.name()) && ctx.kernalContext().clientNode())
                throw new IgniteCheckedException("Test query exception " + ctx.name() + " " + new Random().nextInt());

            return true;
        }
    }
}
