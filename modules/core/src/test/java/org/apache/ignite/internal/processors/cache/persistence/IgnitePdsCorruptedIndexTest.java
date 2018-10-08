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
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Test to reproduce corrupted indexes problem after partition file eviction and truncation.
 */
public class IgnitePdsCorruptedIndexTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE = "cache";

    /** Flag indicates that {@link HaltOnTruncateFileIO} should be used. */
    private boolean haltFileIO;

    /** MultiJVM flag. */
    private boolean multiJvm = true;

    /** Additional remote JVM args. */
    private List<String> additionalArgs = Collections.emptyList();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(10 * 60 * 1000)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(512 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        if (haltFileIO)
            dsCfg.setFileIOFactory(new HaltOnTruncateFileIOFactory(new RandomAccessFileIOFactory()));

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setIndexedTypes(Integer.class, IndexedObject.class, Long.class, IndexedObject.class)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return multiJvm;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return additionalArgs;
    }

    /**
     *
     */
    public void testCorruption() throws Exception {
        final String corruptedNodeName = "corrupted";

        IgniteEx ignite = startGrid(0);

        haltFileIO = true;

        additionalArgs = new ArrayList<>();
        additionalArgs.add("-D" + "TEST_CHECKPOINT_ON_EVICTION=true");
        additionalArgs.add("-D" + "IGNITE_QUIET=false");

        IgniteEx corruptedNode = (IgniteEx) startGrid(corruptedNodeName);

        additionalArgs.clear();

        haltFileIO = false;

        startGrid(2);

        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        final int entityCnt = 3_200;

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(CACHE)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < entityCnt; i++)
                streamer.addData(i, new IndexedObject(i));
        }

        startGrid(3);

        resetBaselineTopology();

        // Corrupted node should be halted during partition destroy.
        GridTestUtils.waitForCondition(() -> ignite.cluster().nodes().size() == 3, getTestTimeout());

        // Clear remote JVM instance cache.
        IgniteProcessProxy.kill(corruptedNode.name());

        stopAllGrids();

        // Disable multi-JVM mode and start coordinator and corrupted node in the same JVM.
        multiJvm = false;

        startGrid(0);

        corruptedNode = (IgniteEx) startGrid(corruptedNodeName);

        corruptedNode.cluster().active(true);

        resetBaselineTopology();

        // If index was corrupted, rebalance or one of the following queries should be failed.
        awaitPartitionMapExchange();

        for (int k = 0; k < entityCnt; k += entityCnt / 4) {
            IgniteCache<Integer, IndexedObject> cache1 = corruptedNode.cache(CACHE);

            int l = k;
            int r = k + entityCnt / 4 - 1;

            log.info("Check range [" + l + "-" + r + "]");

            QueryCursor<Cache.Entry<Long, IndexedObject>> qry =
                cache1.query(new SqlQuery(IndexedObject.class, "lVal between ? and ?")
                    .setArgs(l * l, r * r));

            Collection<Cache.Entry<Long, IndexedObject>> queried = qry.getAll();

            log.info("Qry result size = " + queried.size());
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** Integer indexed value. */
        @QuerySqlField(index = true)
        private int iVal;

        /** Long indexed value. */
        @QuerySqlField(index = true)
        private long lVal;

        /** */
        private byte[] payload = new byte[1024];

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
            this.lVal = (long) iVal * iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }

    /**
     * File I/O which halts JVM after specified file truncation.
     */
    private static class HaltOnTruncateFileIO extends FileIODecorator {
        /** File. */
        private final File file;

        /** The overall number of file truncations have done. */
        private static final AtomicInteger truncations = new AtomicInteger();

        /**
         * @param delegate File I/O delegate
         */
        public HaltOnTruncateFileIO(FileIO delegate, File file) {
            super(delegate);
            this.file = file;
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            super.clear();

            System.err.println("Truncated file: " + file.getAbsolutePath());

            truncations.incrementAndGet();

            Integer checkpointedPart = null;
            try {
                Field field = GridDhtLocalPartition.class.getDeclaredField("partWhereTestCheckpointEnforced");

                field.setAccessible(true);

                checkpointedPart = (Integer) field.get(null);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // Wait while more than one file have truncated and checkpoint on partition eviction has done.
            if (truncations.get() > 1 && checkpointedPart != null) {
                System.err.println("JVM is going to be crushed for test reasons...");

                Runtime.getRuntime().halt(0);
            }
        }
    }

    /**
     * I/O Factory which creates {@link HaltOnTruncateFileIO} instances for partition files.
     */
    private static class HaltOnTruncateFileIOFactory implements FileIOFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegateFactory;

        /**
         * @param delegateFactory Delegate factory.
         */
        HaltOnTruncateFileIOFactory(FileIOFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        /**
         * @param file File.
         */
        private static boolean isPartitionFile(File file) {
            return file.getName().contains("part") && file.getName().endsWith("bin");
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            FileIO delegate = delegateFactory.create(file);

            if (isPartitionFile(file))
                return new HaltOnTruncateFileIO(delegate, file);

            return delegate;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            if (isPartitionFile(file))
                return new HaltOnTruncateFileIO(delegate, file);

            return delegate;
        }
    }
}
