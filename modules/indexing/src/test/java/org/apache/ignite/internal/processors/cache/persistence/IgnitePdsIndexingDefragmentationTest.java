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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Defragmentation tests with enabled ignite-indexing.
 */
public class IgnitePdsIndexingDefragmentationTest extends IgnitePdsDefragmentationTest {
    /** Use MVCC in tests. */
    private static final String USE_MVCC = "USE_MVCC";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setInitialSize(100L * 1024 * 1024)
                .setMaxSize(1024L * 1024 * 1024)
                .setPersistenceEnabled(true)
        );

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<?, ?> cache1Cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setGroupName(GRP_NAME)
            .setIndexedTypes(
                ObjKey.class, byte[].class,
                Integer.class, byte[].class
            )
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        CacheConfiguration<?, ?> cache2Cfg = new CacheConfiguration<>(CACHE_2_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setGroupName(GRP_NAME)
            .setIndexedTypes(
                ObjKey.class, byte[].class,
                Integer.class, byte[].class
            )
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        if (Boolean.TRUE.toString().equals(System.getProperty(USE_MVCC))) {
            cache1Cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
            cache2Cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        } else
            cache2Cfg.setExpiryPolicyFactory(new PolicyFactory());

        cfg.setCacheConfiguration(cache1Cfg, cache2Cfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        GridQueryProcessor.idxCls = null;
    }

    /**
     * Fill cache, remove half of the entries, defragmentate PDS and check index.
     *
     * @param keyMapper Function that provides key based on the index of entry.
     * @param <T> Type of cache key.
     *
     * @throws Exception If failed.
     */
    private <T> void test(Function<Integer, T> keyMapper) throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(keyMapper, ig.cache(DEFAULT_CACHE_NAME));

        forceCheckpoint(ig);

        createMaintenanceRecord();

        stopGrid(0);

        File dbWorkDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);
        File nodeWorkDir = new File(dbWorkDir, U.maskForFileName(ig.name()));
        File workDir = new File(nodeWorkDir, FilePageStoreManager.CACHE_GRP_DIR_PREFIX + GRP_NAME);

        long oldIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();

        startGrid(0);

        long newIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();

        assertTrue(newIdxFileLen <= oldIdxFileLen);

        File completionMarkerFile = DefragmentationFileUtils.defragmentationCompletionMarkerFile(workDir);
        assertTrue(completionMarkerFile.exists());

        stopGrid(0);

        GridQueryProcessor.idxCls = CaptureRebuildGridQueryIndexing.class;

        IgniteEx node = startGrid(0);

        awaitPartitionMapExchange();

        CaptureRebuildGridQueryIndexing indexing = (CaptureRebuildGridQueryIndexing) node.context().query().getIndexing();

        assertFalse(indexing.didRebuildIndexes());

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        assertFalse(completionMarkerFile.exists());

        validateIndexes(node);

        for (int k = 0; k < ADDED_KEYS_COUNT; k++)
            cache.get(keyMapper.apply(k));
    }

    /**
     * Test that indexes are correct.
     *
     * @param node Node.
     * @throws Exception If failed.
     */
    private static void validateIndexes(IgniteEx node) throws Exception {
        ValidateIndexesClosure clo = new ValidateIndexesClosure(
            Collections.singleton(DEFAULT_CACHE_NAME),
            0,
            0,
            false,
            true
        );

        node.context().resource().injectGeneric(clo);

        VisorValidateIndexesJobResult call = clo.call();
        assertFalse(call.hasIssues());
    }

    /**
     * Test using integer keys.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexingWithIntegerKey() throws Exception {
        test(Function.identity());
    }

    /**
     * Test using complex keys (integer and string).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexingWithComplexKey() throws Exception {
        test(integer -> new ObjKey(integer, "test"));
    }

    /**
     * Test using integer keys.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = USE_MVCC, value = "true")
    public void testIndexingWithIntegerKeyAndMVCC() throws Exception {
        test(Function.identity());
    }

    /**
     * Test using complex keys (integer and string).
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = USE_MVCC, value = "true")
    public void testIndexingWithComplexKeyAndMVCC() throws Exception {
        test(integer -> new ObjKey(integer, "test"));
    }

    /**
     * Complex key for cache.
     */
    public static class ObjKey {
        /** */
        private Integer intKey;

        /** */
        private String strKey;

        /** Constructor. */
        public ObjKey(Integer intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ObjKey key = (ObjKey) o;
            return Objects.equals(intKey, key.intKey) &&
                    Objects.equals(strKey, key.strKey);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(intKey, strKey);
        }
    }

    /**
     * IgniteH2Indexing that captures index rebuild operations.
     */
    public static class CaptureRebuildGridQueryIndexing extends IgniteH2Indexing {
        /**
         * Whether index rebuild happened.
         */
        private boolean rebuiltIndexes;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
            IgniteInternalFuture<?> future = super.rebuildIndexesFromHash(cctx);
            rebuiltIndexes = future != null;
            return future;
        }

        /**
         * Get index rebuild flag.
         *
         * @return Whether index rebuild happened.
         */
        public boolean didRebuildIndexes() {
            return rebuiltIndexes;
        }
    }
}
