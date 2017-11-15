/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgnitePutUpdateWalPrintTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Factory. */
    private IgniteWalIteratorFactory factory;

    /** Wal path. */
    private String walPath;


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<String, BinaryObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024)
                    .setPersistenceEnabled(true)
                    .setCheckpointPageBufferSize(DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE + 1))
            .setWalMode(WALMode.DEFAULT)
            .setWalThreadLocalBufferSize(640000000);

        cfg.setDataStorageConfiguration(memCfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        if (gridName.endsWith("1"))
            cfg.setClientMode(true);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));

        super.afterTestsStopped();
    }

    /**
     * @throws Exception On error.
     */
    private void initGrid() throws Exception {
        startGrids(1);

        factory = new IgniteWalIteratorFactory(new NullLogger(),
            grid(0).configuration().getDataStorageConfiguration().getPageSize(),
            null,
            null,
            false);

        grid(0).active(true);

        walPath = GridTestUtils.getFieldValue(grid(0)
            .cachex(CACHE_NAME).context().shared().wal(), "walWorkDir").toString();
    }

    /**
     * @throws Exception if failed.
     */
    public void testPrint() throws Exception {
        initGrid();

        IgniteEx ig = grid(0);

        BinaryObjectBuilder bob = ig.binary().builder("CustomType");

        bob.setField("field0", "val0");
        bob.setField("field1", "val1");
        bob.setField("field2", "val2");

        ig.cache(CACHE_NAME).put("key", bob.build());

        ig.cache(CACHE_NAME).remove("key");


        ig.cache(CACHE_NAME).put("key", bob.build());

        stopAllGrids(false);

        System.out.println("+++ WAL +++");

        printWal();

        System.exit(0);

        ig.cache(CACHE_NAME).<String, BinaryObject>withKeepBinary().invoke("key", new CacheEntryProcessor<String, BinaryObject, Void>() {
            @Override
            public Void process(MutableEntry<String, BinaryObject> entry,
                Object... objects) throws EntryProcessorException {
                BinaryObjectBuilder bob = entry.getValue().toBuilder();

                bob.setField("field0", "val01");

                entry.setValue(bob.build());

                return null;
            }
        });

        stopAllGrids(false);

        System.out.println("+++ WAL +++");

        printWal();
    }

    /**
     * @throws IgniteCheckedException On error.
     */
    private void printWal() throws IgniteCheckedException {
        final File walWorkDirWithConsistentId = new File(walPath);

        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        if (workFiles == null)
            throw new IllegalArgumentException("No .wal files in dir: " + walPath);

        try (WALIterator stIt = factory.iteratorWorkFiles(workFiles)) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                System.out.println("[W] " + next.get2());
            }
        }

//        if (args.length >= 3) {
//            final File walArchiveDirWithConsistentId = new File(args[2]);
//
//            try (WALIterator stIt = factory.iteratorArchiveDirectory(walArchiveDirWithConsistentId)) {
//                while (stIt.hasNextX()) {
//                    IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
//
//                    System.out.println("[A] " + next.get2());
//                }
//            }
//        }
    }
}
