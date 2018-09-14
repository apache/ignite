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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class IgniteChangeGlobalStateAbstractTest extends GridCommonAbstractTest {
    /** Primary suffix. */
    private static final String primarySuffix = "-primary";

    /** BackUp suffix. */
    private static final String backUpSuffix = "-backUp";

    /** BackUp suffix. */
    private static final String clientSuffix = "-client";

    /** Primary ip finder. */
    private final TcpDiscoveryIpFinder primaryIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Back up ip finder. */
    private final TcpDiscoveryIpFinder backUpIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Nodes. */
    protected Map<String, Ignite> nodes = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        nodes.clear();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName(), true));

        startPrimaryNodes(primaryNodes());

        startPrimaryClientNodes(primaryClientNodes());

        startBackUpNodes(backUpNodes());

        startBackUpClientNodes(backUpClientNodes());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAll(clientSuffix);

        stopAll(primarySuffix);

        stopAll(backUpSuffix);

        nodes.clear();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName(), true));
    }

    /**
     * @return Number of server nodes in primary cluster.
     */
    protected int primaryNodes() {
        return 3;
    }

    /**
     * @return Number of client nodes in primary cluster.
     */
    protected int primaryClientNodes() {
        return 3;
    }

    /**
     * @return Number of server nodes in backup cluster.
     */
    protected int backUpNodes() {
        return 3;
    }

    /**
     * @return Number of client nodes in backup cluster.
     */
    protected int backUpClientNodes() {
        return 3;
    }

    /**
     * @param idx idx.
     * @return Primary cluster node.
     */
    protected Ignite primary(int idx) {
        return nodes.get("node" + idx + primarySuffix);
    }

    /**
     * @param idx idx.
     * @return Primary cluster client node.
     */
    Ignite primaryClient(int idx) {
        return nodes.get("node" + idx + primarySuffix + clientSuffix);
    }

    /**
     * @param idx idx.
     * @return Backup cluster node.
     */
    Ignite backUp(int idx) {
        return nodes.get("node" + idx + backUpSuffix);
    }

    /**
     * @param idx idx.
     * @return Backup cluster client node.
     */
    Ignite backUpClient(int idx) {
        return nodes.get("node" + idx + backUpSuffix + clientSuffix);
    }

    /**
     * @param cnt Count.
     * @throws Exception If failed.
     */
    void startPrimaryNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            startPrimary(i);

        if (cnt > 0)
            grid("node0" + primarySuffix).active(true);
    }

    /**
     * @param idx Index.
     * @throws Exception If failed.
     */
    private void startPrimary(int idx) throws Exception {
        String node = "node" + idx;

        String name = node + primarySuffix;

        IgniteConfiguration cfg = getConfiguration(name);
        cfg.setConsistentId(node);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(primaryIpFinder);

        Ignite ig = startGrid(name, cfg);

        nodes.put(name, ig);
    }

    /**
     * @param cnt Count.
     * @throws Exception If failed.
     */
    void startBackUpNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            startBackUp(i);
    }

    /**
     * @param idx Index.
     * @throws Exception If failed.
     */
    void startBackUp(int idx) throws Exception {
        String node = "node" + idx;

        String name = node + backUpSuffix;

        IgniteConfiguration cfg = getConfiguration(name);

        cfg.setConsistentId(node);
        cfg.setAutoActivationEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(backUpIpFinder);

        Ignite ig = startGrid(name, cfg);

        nodes.put(name, ig);
    }

    /**
     * @param cnt Count.
     * @throws Exception If failed.
     */
    void startPrimaryClientNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            String node = "node" + i;

            String name = node + primarySuffix + clientSuffix;

            IgniteConfiguration cfg = getConfiguration(name);

            cfg.setConsistentId(node);
            cfg.setClientMode(true);

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(primaryIpFinder);

            Ignite ig = startGrid(name, cfg);

            nodes.put(name, ig);
        }
    }

    /**
     * @param cnt Count.
     * @throws Exception If failed.
     */
    private void startBackUpClientNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            String node = "node" + i;

            String name = node + backUpSuffix + clientSuffix;

            IgniteConfiguration cfg = getConfiguration(name);
            cfg.setConsistentId(node);
            cfg.setActiveOnStart(false);
            cfg.setClientMode(true);
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(backUpIpFinder);

            Ignite ig = startGrid(name, cfg);

            nodes.put(name, ig);
        }
    }

    /**
     * @return All nodes from backup cluster.
     */
    Iterable<Ignite> allBackUpNodes() {
        List<Ignite> r = new ArrayList<>();

        for (String name : this.nodes.keySet())
            if (name.contains(backUpSuffix))
                r.add(nodes.get(name));

        return r;
    }

    /**
     * @param includeClient If {@code true} then allow to return client.
     * @return Random node from backup topology.
     */
    Ignite randomBackUp(boolean includeClient) {
        List<Ignite> igs = new ArrayList<>();

        for (String name : this.nodes.keySet())
            if (name.contains(backUpSuffix)) {
                if (includeClient)
                    igs.add(this.nodes.get(name));
                else {
                    if (name.contains(clientSuffix))
                        continue;

                    igs.add(this.nodes.get(name));
                }
            }

        int idx = ThreadLocalRandom.current().nextInt(0, igs.size());

        return igs.get(idx);
    }

    /**
     * @param i Idx.
     */
    void stopPrimary(int i) {
        String name = "node" + i + primarySuffix;

        nodes.get(name).close();

        nodes.remove(name);
    }

    /**
     *
     */
    void stopAllPrimary() {
        stopAll(primarySuffix);
    }

    /**
     *
     */
    protected void stopAllBackUp() {
        stopAll(backUpSuffix);
    }

    /**
     *
     */
    protected void stopAllClient() {
        stopAll(clientSuffix);
    }

    /**
     * @param suffix Suffix.
     */
    private void stopAll(String suffix) {
        for (String name : nodes.keySet())
            if (name.contains(suffix)) {
                Ignite ig = nodes.get(name);

                stopGrid(ig.name());

                nodes.remove(name);
            }
    }

    /**
     * @param gridName Grid name.
     */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration pCfg = new DataStorageConfiguration();

        pCfg.setStoragePath(testName() + "/db");
        pCfg.setWalArchivePath(testName() + "/db/wal/archive");
        pCfg.setWalPath(testName() + "/db/wal");

        pCfg.setPageSize(1024);
        pCfg.setConcurrencyLevel(64);

        pCfg.setWalMode(WALMode.LOG_ONLY);

        pCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(200L * 1024 * 1024).setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(pCfg);

        return cfg;
    }

    /**
     * @return Test class name.
     */
    protected String testName() {
        return getClass().getSimpleName();
    }
}
