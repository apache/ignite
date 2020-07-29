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

package org.apache.ignite.yardstick;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriverAdapter;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.yardstickframework.BenchmarkUtils.jcommander;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Abstract class for Ignite benchmarks.
 */
public abstract class IgniteAbstractBenchmark extends BenchmarkDriverAdapter {
    /** Arguments. */
    protected final IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

    /** Logger */
    private PreloadLogger lgr;

    /** Node. */
    private IgniteNode node;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        jcommander(cfg.commandLineArguments(), args, "<ignite-driver>");

        if (Ignition.state() != IgniteState.STARTED) {
            node = new IgniteNode(args.isClientOnly() && !args.isNearCache());

            node.start(cfg);
        }
        else
            // Support for mixed benchmarks mode.
            node = new IgniteNode(args.isClientOnly() && !args.isNearCache(), Ignition.ignite());

        waitForNodes();

        activateCluster();

        IgniteLogger log = ignite().log();

        if (log.isInfoEnabled())
            log.info("Benchmark arguments: " + args);
    }

    /**
     * Checks if persistence is enabled and activates cluster.
     */
    private void activateCluster() {
        //Flag to set if there is at least one data region with persistence in Ignite configuration.
        boolean pdsInCfg = false;

        DataStorageConfiguration dsCfg = ignite().configuration().getDataStorageConfiguration();

        if (dsCfg != null) {
            pdsInCfg = dsCfg.getDefaultDataRegionConfiguration().isPersistenceEnabled();

            DataRegionConfiguration[] drCfgArr = dsCfg.getDataRegionConfigurations();

            if (drCfgArr != null) {
                for (DataRegionConfiguration drCfg : drCfgArr) {
                    if (drCfg.isPersistenceEnabled()) {
                        pdsInCfg = true;

                        break;
                    }
                }
            }
        }

        if ((args.persistentStoreEnabled() || pdsInCfg) && !ignite().cluster().active()) {
            BenchmarkUtils.println("Activating cluster.");

            ignite().cluster().active(true);
        }
    }

    /**
     * Prints non-system caches sizes during preload.
     *
     * @param logInterval time interval between printing preload log. Required to be positive.
     */
    protected void startPreloadLogging(long logInterval) {
        try {
            if (node != null && cfg != null && logInterval >= 0)
                lgr = IgniteBenchmarkUtils.startPreloadLogger(node, cfg, logInterval);
            else
                BenchmarkUtils.println("Failed to start preload logger [node=" + node + ", cfg = " + cfg +
                    ", logInterval = " + logInterval + "]");
        }
        catch (Exception e) {
            BenchmarkUtils.error("Failed to start preload logger [node=" + node + ", cfg = " + cfg +
                ", logInterval = " + logInterval + "]", e);
        }
    }

    /**
     * Terminates printing preload log.
     */
    protected void stopPreloadLogging() {
        if (lgr != null)
            lgr.stopAndPrintStatistics();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (node != null)
            node.stop();
    }

    /** {@inheritDoc} */
    @Override public String description() {
        String desc = BenchmarkUtils.description(cfg, this);

        return desc.isEmpty() ?
            getClass().getSimpleName() + args.description() + cfg.defaultDescription() : desc;
    }

    /** {@inheritDoc} */
    @Override public String usage() {
        return BenchmarkUtils.usage(args);
    }

    /**
     * @return Grid.
     */
    protected Ignite ignite() {
        return node.ignite();
    }

    /**
     * @throws Exception If failed.
     */
    private void waitForNodes() throws Exception {
        final CountDownLatch nodesStartedLatch = new CountDownLatch(1);

        ignite().events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event gridEvt) {
                if (nodesStarted())
                    nodesStartedLatch.countDown();

                return true;
            }
        }, EVT_NODE_JOINED);

        if (!nodesStarted()) {
            println(cfg, "Waiting for the cluster to contain at least " + args.nodes() + " nodes...");

            nodesStartedLatch.await();
        }

        println("Cluster is ready");
    }

    /**
     * Determine if all required nodes are started. Since nodes can close their local ignite instances, this method
     * seeks in the history topology containing: 1) driver's local node; 2) right number of nodes.
     *
     * @return {@code True} if all nodes are started, {@code false} otherwise.
     */
    private boolean nodesStarted() {
        IgniteCluster cluster = ignite().cluster();

        UUID locNodeId = cluster.localNode().id();

        long curTop = cluster.topologyVersion();

        for (long top = curTop; top >= 1; top--) {
            Collection<ClusterNode> nodes = cluster.topology(top);

            // Current node don't know about such topology because it joined later.
            if (nodes == null)
                continue;

            if (topologyContainsId(nodes, locNodeId) && nodes.size() >= args.nodes())
                return true;
        }

        return false;
    }

    /**
     * @param top topology (collection of cluster nodes).
     * @param nodeId id of the node to find.
     * @return {@code True} if topology contains node with specified id, {@code false} otherwise.
     */
    private static boolean topologyContainsId(@NotNull Collection<? extends ClusterNode> top, UUID nodeId) {
        for (ClusterNode node : top) {
            if (node.id().equals(nodeId))
                return true;
        }

        return false;
    }

    /**
     * @param max Key range.
     * @return Next key.
     */
    public static int nextRandom(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }

    /**
     * @param min Minimum key in range.
     * @param max Maximum key in range.
     * @return Next key.
     */
    protected int nextRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(max - min) + min;
    }
}
