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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.lang.String.valueOf;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class LocalWacModeNoChangeDuringRebalanceOnNonNodeAssignTest extends GridCommonAbstractTest {

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalPath(walPath(name))
                .setWalArchivePath(walArchivePath(name))
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(
                    new RendezvousAffinityFunction(false, 3))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        System.setProperty(IGNITE_DISABLE_WAL_DURING_REBALANCING, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IGNITE_DISABLE_WAL_DURING_REBALANCING);
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        Ignite ig = startGrids(NODES);

        ig.cluster().active(true);

        int entries = 100_000;

        try (IgniteDataStreamer<Integer, Integer> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < entries; i++)
                st.addData(i, -i);
        }

        IgniteEx ig4 = startGrid(NODES);

        ig4.cluster().setBaselineTopology(ig4.context().discovery().topologyVersion());

        IgniteWalIteratorFactory iteratorFactory = new IgniteWalIteratorFactory(log);

        String name = ig4.name();

        try (WALIterator it = iteratorFactory.iterator(walPath(name), walArchivePath(name))) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                WALRecord rec = tup.get2();

                if (rec.type() == METASTORE_DATA_RECORD) {
                    MetastoreDataRecord metastoreDataRecord = (MetastoreDataRecord)rec;

                    String key = metastoreDataRecord.key();

                    if (key.startsWith("grp-wal-") &&
                        key.contains(valueOf(cacheId(DEFAULT_CACHE_NAME))) &&
                        metastoreDataRecord.value() != null)
                        fail("WAL was disabled but should not.");
                }
            }
        }
    }

    /**
     *
     * @param nodeName Node name.
     * @return Path to WAL work directory.
     * @throws IgniteCheckedException If failed.
     */
    private String walPath(String nodeName) throws IgniteCheckedException {
        String workDir = U.defaultWorkDirectory();

        return workDir + "/" + DFLT_STORE_DIR + "/" + nodeName + "/wal";
    }

    /**
     *
     * @param nodeName Node name.
     * @return Path to WAL archive directory.
     * @throws IgniteCheckedException If failed.
     */
    private String walArchivePath(String nodeName) throws IgniteCheckedException {
        String workDir = U.defaultWorkDirectory();

        return workDir + "/" + DFLT_STORE_DIR + "/" + nodeName + "/walArchive";
    }
}
