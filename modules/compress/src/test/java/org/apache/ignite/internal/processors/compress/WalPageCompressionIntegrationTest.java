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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 *
 */
public class WalPageCompressionIntegrationTest extends AbstractPageCompressionIntegrationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setWalPageCompression(compression)
            .setWalPageCompressionLevel(compressionLevel);

        return super.getConfiguration(igniteName)
            .setDataStorageConfiguration(dsCfg)
            // Set new IP finder for each node to start independent clusters.
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder(true)));
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void doTestPageCompression() throws Exception {
        // Ignite instance with compressed WAL page records.
        IgniteEx ignite0 = startGrid(0);

        compression = DiskPageCompression.DISABLED;
        compressionLevel = null;

        // Reference ignite instance with uncompressed WAL page records.
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);
        ignite1.cluster().active(true);

        String cacheName = "test";

        CacheConfiguration<Integer, TestVal> ccfg = new CacheConfiguration<Integer, TestVal>()
            .setName(cacheName)
            .setBackups(0)
            .setAtomicityMode(ATOMIC)
            .setIndexedTypes(Integer.class, TestVal.class);

        IgniteCache<Integer,TestVal> cache0 = ignite0.getOrCreateCache(ccfg);
        IgniteCache<Integer,TestVal> cache1 = ignite1.getOrCreateCache(ccfg);

        int cnt = 20_000;

        for (int i = 0; i < cnt; i++) {
            assertTrue(cache0.putIfAbsent(i, new TestVal(i)));
            assertTrue(cache1.putIfAbsent(i, new TestVal(i)));
        }

        for (int i = 0; i < cnt; i += 2) {
            assertEquals(new TestVal(i), cache0.getAndRemove(i));
            assertEquals(new TestVal(i), cache1.getAndRemove(i));
        }

        // Write any WAL record to get current WAL pointers.
        FileWALPointer ptr0 = (FileWALPointer)ignite0.context().cache().context().wal().log(new CheckpointRecord(null));
        FileWALPointer ptr1 = (FileWALPointer)ignite1.context().cache().context().wal().log(new CheckpointRecord(null));

        log.info("Compressed WAL pointer: " + ptr0);
        log.info("Uncompressed WAL pointer: " + ptr1);

        assertTrue("Compressed WAL must be smaller than uncompressed [ptr0=" + ptr0 + ", ptr1=" + ptr1 + ']',
            ptr0.compareTo(ptr1) < 0);
    }
}
