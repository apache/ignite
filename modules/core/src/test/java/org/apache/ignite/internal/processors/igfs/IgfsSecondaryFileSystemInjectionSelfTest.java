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

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.FileSystemResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for resource injection to secondary file system.
 */
public class IgfsSecondaryFileSystemInjectionSelfTest extends GridCommonAbstractTest {
    /** IGFS name. */
    protected static final String IGFS_NAME = "igfs-test";

    /** Test implementation of secondary filesystem. */
    private TestBaseSecondaryFsMock secondary;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName(IGFS_NAME);
        igfsCfg.setDefaultMode(IgfsMode.DUAL_SYNC);
        igfsCfg.setSecondaryFileSystem(secondary);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        igfsCfg.setDataCacheConfiguration(dataCacheCfg);
        igfsCfg.setMetaCacheConfiguration(metaCacheCfg);

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public void testInjectPrimaryByField() throws Exception {
        secondary = new TestBaseSecondaryFsMock() {
            @FileSystemResource
            private IgfsImpl igfs;

            @LoggerResource
            private IgniteLogger log;

            @IgniteInstanceResource
            private Ignite ig;

            @Override boolean checkInjection(Ignite ignite, IgniteFileSystem primary) {
                return igfs == primary && log instanceof IgniteLogger && ig == ignite;
            }
        };

        Ignite ig = startGrid(0);

        IgniteFileSystem igfs = ig.fileSystem(IGFS_NAME);

        assert secondary.checkInjection(ig, igfs);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public void testInjectPrimaryByMethods() throws Exception {
        secondary = new TestBaseSecondaryFsMock() {
            /** Ignite instance. */
            private Ignite ig;

            /** IGFS instance. */
            private IgniteFileSystem igfs;

            /** Logger injected flag */
            private boolean logSet;

            /**
             * @param igfs Primary IGFS.
             */
            @FileSystemResource
            void setPrimaryIgfs(IgfsImpl igfs) {
                this.igfs = igfs;
            }

            /**
             * @param log Ignite logger.
             */
            @LoggerResource
            void setIgLogger(IgniteLogger log) {
                logSet = log instanceof IgniteLogger;
            }

            /**
             * @param ig Ignite instance.
             */
            @IgniteInstanceResource
            void setIgniteInst(Ignite ig) {
                this.ig = ig;
            }

            @Override boolean checkInjection(Ignite ignite, IgniteFileSystem primary) {
                return ignite == ig && primary == igfs && logSet;
            }
        };

        Ignite ig = startGrid(0);

        IgniteFileSystem igfs = ig.fileSystem(IGFS_NAME);

        assert secondary.checkInjection(ig, igfs);
    }

    /**
     *
     */
    private static abstract class TestBaseSecondaryFsMock implements IgfsSecondaryFileSystem {

        /** {@inheritDoc} */
        @Override public boolean exists(IgfsPath path) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void rename(IgfsPath src, IgfsPath dest) throws IgniteException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean delete(IgfsPath path, boolean recursive) throws IgniteException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(IgfsPath path) throws IgniteException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) throws IgniteException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public OutputStream create(IgfsPath path, boolean overwrite) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication, long blockSize,
            @Nullable Map<String, String> props) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
            @Nullable Map<String, String> props) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgfsFile info(IgfsPath path) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long usedSpaceSize() throws IgniteException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void setTimes(IgfsPath path, long modificationTime, long accessTime) throws IgniteException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len,
            long maxLen) throws IgniteException {
            return null;
        }

        /**
         * @param ignite Ignite instance.
         * @param primary Primary IGFS.
         * @return {@code True} if injection is correct.
         */
        abstract boolean checkInjection(Ignite ignite, IgniteFileSystem primary);
    }
}
