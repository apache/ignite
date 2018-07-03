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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 *
 */
public class IgniteWalIteratorExceptionDuringReadTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int WAL_SEGMENT_SIZE = 1024 * 1024 * 20;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        IgniteEx ig = (IgniteEx)startGrid();

        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20 * 4; i++)
            cache.put(i, new byte[1024 * 1024]);

        ig.cluster().active(false);

        IgniteWalIteratorFactory iteratorFactory = new IgniteWalIteratorFactory(log);

        FileWALPointer failOnPtr = new FileWALPointer(3, 1024 * 1024 * 5, 0);

        String failMessage = "test fail message";

        IteratorParametersBuilder builder = new IteratorParametersBuilder()
            .filesOrDirs(U.defaultWorkDirectory())
            .filter((r, ptr) -> {
                FileWALPointer ptr0 = (FileWALPointer)ptr;

                if (ptr0.compareTo(failOnPtr) >= 0)
                    throw new TestRuntimeException(failMessage);

                return true;
            });

        try (WALIterator it = iteratorFactory.iterator(builder)) {
            FileWALPointer ptr = null;

            boolean failed = false;

            while (it.hasNext()) {
                try {
                    IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                    ptr = (FileWALPointer)tup.get1();
                }
                catch (IgniteException e) {
                    Assert.assertNotNull(ptr);
                    Assert.assertEquals(failOnPtr.index(), ptr.index());
                    Assert.assertTrue(ptr.compareTo(failOnPtr) < 0);

                    failed = X.hasCause(e, TestRuntimeException.class);

                    break;
                }
            }

            assertTrue(failed);
        }
    }

    /**
     *
     */
    private static class TestRuntimeException extends IgniteException {
        /**
         * @param msg Exception message.
         */
        private TestRuntimeException(String msg) {
            super(msg);
        }
    }
}
