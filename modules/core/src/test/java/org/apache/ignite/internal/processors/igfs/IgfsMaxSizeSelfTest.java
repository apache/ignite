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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.internal.IgniteEx;

/**
 * Check max size limit.
 */
@SuppressWarnings("ConstantConditions")
public class IgfsMaxSizeSelfTest extends IgfsCommonAbstractTest {
    /** Work directory. */
    private static long maxSize;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName("test");

        if (maxSize > 0)
            igfsCfg.setMaxSpaceSize(maxSize);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setCacheMode(CacheMode.REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        igfsCfg.setDataCacheConfiguration(dataCacheCfg);
        igfsCfg.setMetaCacheConfiguration(metaCacheCfg);

        cfg.setFileSystemConfiguration(igfsCfg);
        cfg.setIgniteInstanceName(igniteInstanceName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultOrZero() throws  Exception {
        IgniteEx ig = startGrid(0);

        try {
            assertEquals(0, ((IgfsImpl)ig.igfsx("test")).globalSpace().spaceTotal());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNegative() throws  Exception {
        maxSize = -1;

        IgniteEx ig = startGrid(0);

        try {
            assertEquals(0, ((IgfsImpl)ig.igfsx("test")).globalSpace().spaceTotal());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPositive() throws  Exception {
        maxSize = 1 << 20;

        IgniteEx ig = startGrid(0);

        try {
            assertEquals(maxSize, ((IgfsImpl)ig.igfsx("test")).globalSpace().spaceTotal());
        }
        finally {
            stopAllGrids();
        }
    }
}