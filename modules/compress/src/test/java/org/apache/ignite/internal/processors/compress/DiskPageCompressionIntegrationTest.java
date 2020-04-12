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

package org.apache.ignite.internal.processors.compress;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;
import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;
import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 *
 */
public class DiskPageCompressionIntegrationTest extends AbstractPageCompressionIntegrationTest {
    /** */
    private FileIOFactory factory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteName) throws Exception {
        DataRegionConfiguration drCfg = new DataRegionConfiguration()
            .setPersistenceEnabled(true);

        factory = getFileIOFactory();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setPageSize(MAX_PAGE_SIZE)
            .setDefaultDataRegionConfiguration(drCfg)
            .setFileIOFactory(U.isLinux() ? factory : new PunchFileIOFactory(factory));

        return super.getConfiguration(igniteName).setDataStorageConfiguration(dsCfg);
    }

    /**
     * @return File IO factory.
     */
    protected FileIOFactory getFileIOFactory() {
        return new RandomAccessFileIOFactory();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void doTestPageCompression() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        String cacheName = "test";

        CacheConfiguration<Integer,TestVal> ccfg = new CacheConfiguration<Integer,TestVal>()
            .setName(cacheName)
            .setBackups(0)
            .setAtomicityMode(ATOMIC)
            .setIndexedTypes(Integer.class, TestVal.class)
            .setDiskPageCompression(compression)
            .setDiskPageCompressionLevel(compressionLevel);

        IgniteCache<Integer,TestVal> cache = ignite.getOrCreateCache(ccfg);

        int cnt = 2_000;

        for (int i = 0; i < cnt; i++)
            assertTrue(cache.putIfAbsent(i, new TestVal(i)));

        for (int i = 0; i < cnt; i += 2)
            assertEquals(new TestVal(i), cache.getAndRemove(i));

        GridCacheDatabaseSharedManager dbMgr = ((GridCacheDatabaseSharedManager)ignite.context()
            .cache().context().database());

        dbMgr.forceCheckpoint("test compression").futureFor(FINISHED).get();

        FilePageStoreManager storeMgr = dbMgr.getFileStoreManager();

        checkFileIOFactory(storeMgr.getPageStoreFileIoFactory());

        Thread.sleep(100); // Wait for metrics update.

        long storeSize = ignite.dataStorageMetrics().getStorageSize();
        long sparseStoreSize = ignite.dataStorageMetrics().getSparseStorageSize();

        assertTrue("storeSize: " + storeSize, storeSize > 0);

        if (U.isLinux()) {
            assertTrue("sparseSize: " + sparseStoreSize, sparseStoreSize > 0);
            assertTrue(storeSize + " > " + sparseStoreSize, storeSize > sparseStoreSize);
        }
        else
            assertTrue(sparseStoreSize < 0);

        GridCacheContext<?,?> cctx = ignite.cachex(cacheName).context();

        int cacheId = cctx.cacheId();
        int groupId = cctx.groupId();

        assertEquals(cacheId, groupId);

        MetricRegistry mreg = ignite.context().metric().registry(
            metricName(CACHE_GROUP_METRICS_PREFIX, cctx.group().cacheOrGroupName()));

        storeSize = mreg.<LongMetric>findMetric("StorageSize").value();
        sparseStoreSize = mreg.<LongMetric>findMetric("SparseStorageSize").value();

        assertTrue("storeSize: " + storeSize, storeSize > 0);

        if (U.isLinux()) {
            assertTrue("sparseSize: " + sparseStoreSize, sparseStoreSize > 0);
            assertTrue(storeSize + " > " + sparseStoreSize, storeSize > sparseStoreSize);
        }
        else
            assertTrue(sparseStoreSize < 0);

        int parts = cctx.affinity().partitions();

        for (int i = 0; i < parts; i++) {
            PageStore store = storeMgr.getStore(cacheId, i);

            long realSize = store.size();
            long virtualSize = store.getPageSize() * store.pages();
            long sparseSize = store.getSparseSize();

            assertTrue(virtualSize > 0);

            error("virt: " + virtualSize + ",  real: " + realSize + ",  sparse: " + sparseSize);

            if (!store.exists())
                continue;

            if (virtualSize > sparseSize)
                return;
        }

        fail("No files were compacted.");
    }

    /**
     */
    public void _testCompressionRatio() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        String cacheName = "test";

        CacheConfiguration<Integer,TestVal> ccfg = new CacheConfiguration<Integer,TestVal>()
            .setName(cacheName)
            .setBackups(0)
            .setAtomicityMode(ATOMIC)
            .setIndexedTypes(Integer.class, TestVal.class)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(10))
            .setDiskPageCompression(ZSTD);
//            .setDiskPageCompressionLevel(compressionLevel);

        ignite.getOrCreateCache(ccfg);

        IgniteInternalCache<Integer,TestVal> cache = ignite.cachex(cacheName);

        MetricRegistry mreg = ignite.context().metric().registry(
            metricName(CACHE_GROUP_METRICS_PREFIX, cacheName));

        GridCacheDatabaseSharedManager dbMgr = ((GridCacheDatabaseSharedManager)ignite.context()
            .cache().context().database());

        int cnt = 20_000_000;

        for (int i = 0; i < cnt; i++) {
            assertTrue(cache.putIfAbsent(i, new TestVal(i)));

            if (i % 50_000 == 0) {
                dbMgr.forceCheckpoint("test").futureFor(FINISHED).get();

                long sparse = mreg.<LongMetric>findMetric("SparseStorageSize").value();
                long size = mreg.<LongMetric>findMetric("StorageSize").value();

                System.out.println(i + " >> " + sparse + " / " + size + " = " + ((double)sparse / size));
            }
        }
    }

    /**
     * @param f Factory.
     */
    protected void checkFileIOFactory(FileIOFactory f) {
        if (!U.isLinux())
            f = ((PunchFileIOFactory)f).delegate;

        assertSame(factory, f);
    }

    /**
     */
    static class PunchFileIO extends FileIODecorator {
        /** */
        private ConcurrentMap<Long, Integer> holes = new ConcurrentHashMap<>();

        /**
         * @param delegate File I/O delegate
         */
        public PunchFileIO(FileIO delegate) {
            super(Objects.requireNonNull(delegate));
        }

        /** {@inheritDoc} */
        @Override public int getFileSystemBlockSize() {
            assertFalse(U.isLinux());

            return 4 * 1024;
        }

        /** {@inheritDoc} */
        @Override public long getSparseSize() {
            assertFalse(U.isLinux());

            long holesSize = holes.values().stream().mapToLong(x -> x).sum();

            try {
                return size() - holesSize;
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
            assertFalse(U.isLinux());

            holes.remove(position);
            return super.writeFully(srcBuf, position);
        }

        /** {@inheritDoc} */
        @Override public int punchHole(long pos, int len) {
            assertFalse(U.isLinux());

            assertTrue(len > 0);

            int blockSize = getFileSystemBlockSize();

            len = len / blockSize * blockSize;

            if (len > 0)
                holes.put(pos, len);

            return len;
        }
    }

    /**
     */
    static class PunchFileIOFactory implements FileIOFactory {
        /** */
        final FileIOFactory delegate;

        /**
         * @param delegate Delegate.
         */
        PunchFileIOFactory(FileIOFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            return new PunchFileIO(delegate.create(file));
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new PunchFileIO(delegate.create(file, modes));
        }
    }
}
