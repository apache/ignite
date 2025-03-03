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

import java.io.File;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.String.valueOf;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;

/**
 *
 */
@WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_REBALANCING, value = "true")
public class LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalPath(walPath(name))
                .setWalArchivePath(walArchivePath(name))
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(atomicityMode)
                .setAffinity(new CustomAffinityFunction(getTestIgniteInstanceName(NODES)))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomic() throws Exception {
        atomicityMode = CacheAtomicityMode.ATOMIC;

        check();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTx() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        check();
    }

    /**
     * @throws Exception If failed.
     */
    public void check() throws Exception {
        Ignite ig = startGridsMultiThreaded(NODES);

        int entries = 100_000;

        try (IgniteDataStreamer<Integer, Integer> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < entries; i++)
                st.addData(i, -i);
        }

        IgniteEx ig4 = startGrid(NODES);

        ig4.cluster().setBaselineTopology(ig4.context().discovery().topologyVersion());

        IgniteWalIteratorFactory iterFactory = new IgniteWalIteratorFactory(log);

        String name = ig4.name();

        try (WALIterator it = iterFactory.iterator(walPath(name), walArchivePath(name))) {
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
     * @param nodeName Node name.
     * @return Path to WAL work directory.
     */
    private String walPath(String nodeName) {
        return new File(sharedFileTree().db(), nodeName + "/wal").getAbsolutePath();
    }

    /**
     * @param nodeName Node name.
     * @return Path to WAL archive directory.
     */
    private String walArchivePath(String nodeName) {
        return new File(sharedFileTree().db(), nodeName + "/walArchive").getAbsolutePath();
    }

    /**
     * Custom affinity function which does not allow to assign partitions to the node with id equals to {@code NODES}.
     */
    public static class CustomAffinityFunction extends RendezvousAffinityFunction {
        /** Consistent id of a node that should not be assigned as primary/backup node. */
        private final String consistentId;

        /**
         * Creates a new instance of CustomAffinityFunction.
         */
        public CustomAffinityFunction(String consistentId) {
            super(false, NODES);
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(final AffinityFunctionContext affCtx) {
            AffinityFunctionContext proxy = new AffinityFunctionContext() {
                /** {@inheritDoc} */
                @Override public @Nullable List<ClusterNode> previousAssignment(int part) {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public int backups() {
                    return affCtx.backups();
                }

                /** {@inheritDoc} */
                @Override public List<ClusterNode> currentTopologySnapshot() {
                    List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

                    nodes.removeIf(n -> n.consistentId().toString().equals(consistentId));

                    return nodes;
                }

                /** {@inheritDoc} */
                @Override public AffinityTopologyVersion currentTopologyVersion() {
                    return affCtx.currentTopologyVersion();
                }

                /** {@inheritDoc} */
                @Override public @Nullable DiscoveryEvent discoveryEvent() {
                    return affCtx.discoveryEvent();
                }
            };

            return super.assignPartitions(proxy);
        }
    }
}
