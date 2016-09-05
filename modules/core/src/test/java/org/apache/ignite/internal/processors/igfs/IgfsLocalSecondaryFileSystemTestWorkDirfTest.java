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

import java.io.File;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.local.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteEx;

/**
 * Check work directory formats.
 */
public class IgfsLocalSecondaryFileSystemTestWorkDirfTest extends IgfsCommonAbstractTest {
    /** Work directory. */
    private static String workDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        LocalIgfsSecondaryFileSystem second = new LocalIgfsSecondaryFileSystem();

        second.setWorkDirectory(workDir);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName("test");
        igfsCfg.setSecondaryFileSystem(second);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        dataCacheCfg.setOffHeapMaxMemory(0);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setCacheMode(CacheMode.REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);
        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAbsoluteWorkDir() throws  Exception {
        File fWork = new File("work/fs/abswork");
        workDir = fWork.getAbsolutePath();
        fWork.mkdirs();
        IgniteEx ig = startGrid(0);

        try {
            IgfsPath p = new IgfsPath("/dir");
            ig.igfsx("test").mkdirs(p);

            assertTrue(new File(fWork, "dir").exists());
        } finally {
            ig.igfsx("test").format();
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRelativeWorkDir() throws  Exception {
        workDir = "work/fs/relwork";
        File fWork = new File(workDir);
        fWork.mkdirs();
        IgniteEx ig = startGrid(0);

        try {
            IgfsPath p = new IgfsPath("/dir");
            ig.igfsx("test").mkdirs(p);

            assertTrue(new File(fWork, "dir").exists());
        } finally {
            ig.igfsx("test").format();
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testURIWorkDir() throws  Exception {
        File fWork = new File("work/fs/absuri");
        workDir = "file://" + fWork.getAbsolutePath();
        fWork.mkdirs();
        IgniteEx ig = startGrid(0);

        try {
            IgfsPath p = new IgfsPath("/dir");
            ig.igfsx("test").mkdirs(p);

            assertTrue(new File(fWork, "dir").exists());
        } finally {
            ig.igfsx("test").format();
            stopAllGrids();
        }
    }
}