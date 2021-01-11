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
package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.LinkedList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/** */
public class WalEnableDisableWithNodeShutdownTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "MY_CACHE";

    /** */
    private static final String CACHE_NAME_2 = "MY_CACHE_2";

    /** */
    private static final int CYCLES = 3;

    /** */
    public static final int NODES = 3;

    /** */
    public static final int WAIT_MILLIS = 150;

    /**
     * Checks whether a node may be down while WAL is disabled on cache.
     *
     * Starts nodes with two caches configured. Activates cluster.
     *
     * Repeats the following:
     * Remove first node.
     * Disable WAL on cache.
     * Bring back first node as new last.
     * Enable WAL on cache.
     */
    @Test
    public void testDisableWhileNodeOffline() throws Exception {
        LinkedList<Ignite> nodes = new LinkedList<>();

        for (int i = 0; i < NODES; i++)
            nodes.add(Ignition.start(igniteCfg(false, "server_" + i)));

        nodes.getFirst().active(true);

        Ignite client = Ignition.start(igniteCfg(true, "client"));

        for (int i = 0; i < CYCLES; i++) {
            try {
                Thread.sleep(WAIT_MILLIS);

                Ignite ignite = nodes.removeFirst();

                String consistentId = (String)ignite.cluster().localNode().consistentId();

                ignite.close();

                Thread.sleep(1_000);

                client.cluster().disableWal(CACHE_NAME);

                Thread.sleep(1_000);

                nodes.add(Ignition.start(igniteCfg(false, consistentId)));

                Thread.sleep(1_000);

                client.cluster().enableWal(CACHE_NAME);
            }
            catch (IgniteException ex) {
                if (ex.getMessage().contains("Operation result is unknown because nodes reported different results")) {
                    log.error(ex.toString(), ex);

                    fail("WAL is in inconsistent state");
                }
                else
                    throw ex;
            }
        }
    }

    /**
     * Checks whether a node may be down while WAL is enabled on cache.
     *
     * Starts nodes with two caches configured. Activates cluster.
     *
     * Repeats the following:
     * Disable WAL on cache.
     * Remove first node.
     * Enable WAL on cache.
     * Bring back first node as new last.
     */
    @Test
    public void testEnableWhileNodeOffline() throws Exception {
        LinkedList<Ignite> nodes = new LinkedList<>();

        for (int i = 0; i < NODES; i++)
            nodes.add(Ignition.start(igniteCfg(false, "server_" + i)));

        nodes.getFirst().active(true);

        Ignite client = Ignition.start(igniteCfg(true, "client"));

        for (int i = 0; i < CYCLES; i++) {
            try {
                Thread.sleep(1_000);

                client.cluster().disableWal(CACHE_NAME);

                Thread.sleep(1_000);

                Ignite ignite = nodes.removeFirst();

                String consistentId = (String)ignite.cluster().localNode().consistentId();

                ignite.close();

                Thread.sleep(1_000);

                client.cluster().enableWal(CACHE_NAME);

                Thread.sleep(1_000);

                nodes.add(Ignition.start(igniteCfg(false, consistentId)));
            }
            catch (IgniteException ex) {
                if (ex.getMessage().contains("Operation result is unknown because nodes reported different results")) {
                    log.error(ex.toString(), ex);

                    fail("WAL is in inconsistent state");
                }
                else
                    throw ex;
            }
        }
    }

    /** */
    @After
    public void cleanUp() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    private IgniteConfiguration igniteCfg(boolean client, String name) throws Exception {
        IgniteConfiguration igniteCfg = getConfiguration(name);

        igniteCfg.setConsistentId(name);

        igniteCfg.setClientMode(client);

        CacheConfiguration configuration = new CacheConfiguration(CACHE_NAME);
        configuration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        configuration.setBackups(0);
        configuration.setCacheMode(CacheMode.PARTITIONED);

        CacheConfiguration configuration2 = new CacheConfiguration(CACHE_NAME_2);
        configuration2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        configuration2.setBackups(0);
        configuration2.setCacheMode(CacheMode.PARTITIONED);

        igniteCfg.setCacheConfiguration(configuration, configuration2);

        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        return igniteCfg;
    }
}
