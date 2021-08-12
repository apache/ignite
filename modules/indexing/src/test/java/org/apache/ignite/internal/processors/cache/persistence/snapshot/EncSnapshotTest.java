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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 * Cluster-wide snapshot test with indexes.
 */
public class EncSnapshotTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<Integer, Object>()
            .setName("CACHE1")
            .setEncryptionEnabled(true)
            .setAffinity(new RendezvousAffinityFunction(1, null));

        cfg.setCacheConfiguration(ccfg);

        cfg
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(3000)
                .setPageSize(DFLT_PAGE_SIZE))
            .setClusterStateOnStart(INACTIVE);

        cfg.getDataStorageConfiguration().setCheckpointFrequency(DFLT_CHECKPOINT_FREQ);

        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
        encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

        cfg.setEncryptionSpi(encSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ClusterState.ACTIVE);

//        ignite.cache("CACHE1").put(1, "hello world!");

//        forceCheckpoint();

        ignite.snapshot().createSnapshot("SNAPSHOT1")
            .get();

        stopAllGrids();
    }

}
