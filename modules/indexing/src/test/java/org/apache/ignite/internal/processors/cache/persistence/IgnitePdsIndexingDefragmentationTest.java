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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.DEFRAGMENTATION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

public class IgnitePdsIndexingDefragmentationTest extends IgnitePdsDefragmentationTest {

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
                .setIndexedTypes(Integer.class, byte[].class)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        CacheConfiguration<?, ?> cache2Cfg = new CacheConfiguration<>(CACHE_2_NAME)
                .setAtomicityMode(TRANSACTIONAL)
                .setGroupName(GRP_NAME)
                .setIndexedTypes(Integer.class, byte[].class)
                .setExpiryPolicyFactory(new PolicyFactory())
                .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        cfg.setCacheConfiguration(cache1Cfg, cache2Cfg);

        return cfg;
    }

    /** */
    @Test
    public void testIndexing() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        ig.context().cache().context().database()
                .forceCheckpoint("test")
                .futureFor(CheckpointState.FINISHED)
                .get();

        stopGrid(0);

        File dbWorkDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);
        File nodeWorkDir = new File(dbWorkDir, U.maskForFileName(ig.name()));
        File workDir = new File(nodeWorkDir, FilePageStoreManager.CACHE_GRP_DIR_PREFIX + GRP_NAME);

        long[] oldPartLen = partitionSizes(workDir);

        long oldIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();

        System.setProperty(DEFRAGMENTATION, "true");

        try {
            startGrid(0);
        }
        finally {
            System.clearProperty(DEFRAGMENTATION);
        }

        awaitPartitionMapExchange();

        long[] newPartLen = partitionSizes(workDir);

        for (int p = 0; p < PARTS; p++)
            assertTrue(newPartLen[p] < oldPartLen[p]);

        long newIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();

        assertTrue(newIdxFileLen <= oldIdxFileLen);

        File completionMarkerFile = DefragmentationFileUtils.defragmentationCompletionMarkerFile(workDir);
        assertTrue(completionMarkerFile.exists());

        stopGrid(0);

        IgniteEx node = startGrid(0);

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        assertFalse(completionMarkerFile.exists());

        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(DEFAULT_CACHE_NAME), 0, 0, false, true);

        node.context().resource().injectGeneric(clo);

        assertFalse(clo.call().hasIssues());

        for (int k = 0; k < ADDED_KEYS_COUNT; k++)
            cache.get(k);
    }

}
