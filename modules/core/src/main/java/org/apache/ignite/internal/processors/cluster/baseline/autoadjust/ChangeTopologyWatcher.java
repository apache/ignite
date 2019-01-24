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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

/**
 * Watcher of topology changes. It initiate to set new baseline after some timeout.
 */
public class ChangeTopologyWatcher implements GridLocalEventListener {
    /** Task represented NULL value is using when normal task can not be created. */
    private static final BaselineAutoAdjustData NULL_BASELINE_DATA = new BaselineAutoAdjustData(null, -1);
    /** */
    private final IgniteLogger log;
    /** */
    private final IgniteClusterImpl cluster;
    /** */
    private final GridCachePartitionExchangeManager<?, ?> exchangeManager;
    /** Configuration of baseline. */
    private final DistributedBaselineConfiguration baselineConfiguration;
    /** Discovery manager. */
    private final GridDiscoveryManager discoveryMgr;
    /** Scheduler of specific task of baseline changing. */
    private final BaselineAutoAdjustScheduler baselineAutoAdjustScheduler;

    /** Last data for set new baseline. */
    private BaselineAutoAdjustData lastBaselineData = NULL_BASELINE_DATA;

    /**
     * @param ctx Context.
     */
    public ChangeTopologyWatcher(GridKernalContext ctx) {
        this.log = ctx.log(ChangeTopologyWatcher.class);
        this.cluster = ctx.cluster().get();
        this.baselineConfiguration = cluster.baselineConfiguration();
        this.exchangeManager = ctx.cache().context().exchange();
        this.baselineAutoAdjustScheduler = new BaselineAutoAdjustScheduler(ctx.timeout(), new BaselineAutoAdjustExecutor(
            ctx.log(BaselineAutoAdjustExecutor.class),
            cluster,
            ctx.getSystemExecutorService(),
            this::isBaselineAutoAdjustEnabled
        ));
        this.discoveryMgr = ctx.discovery();

        ctx.internalSubscriptionProcessor().registerChangeStateListener((msg) -> {
            boolean isBaselineChangedManually = msg.success() && !msg.isBaselineAutoAdjust();

            if (isBaselineChangedManually && isBaselineAutoAdjustEnabled() && isLocalNodeCoordinator()
                && !isBaselineEqualToGrid())
                disableBaselineAutoAdjust("manual baseline change was detected");
        });
    }

    /**
     * @return {@code true} If baseline nodes equal to actual grid nodes.
     */
    private boolean isBaselineEqualToGrid() {
        return eqNotOrdered(cluster.currentBaselineTopology(), new ArrayList<>(cluster.forServers().nodes()));
    }

    /** {@inheritDoc} */
    @Override public void onEvent(Event evt) {
        if (!isBaselineAutoAdjustEnabled()) {
            lastBaselineData = NULL_BASELINE_DATA;

            return;
        }

        synchronized (this) {
            lastBaselineData = lastBaselineData.next(evt, ((DiscoveryEvent)evt).topologyVersion());

            if (isLocalNodeCoordinator()) {
                exchangeManager.affinityReadyFuture(new AffinityTopologyVersion(((DiscoveryEvent)evt).topologyVersion()))
                    .listen((IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>)future -> {

                        if (exchangeManager.lastFinishedFuture().hasLostPartitions()) {
                            disableBaselineAutoAdjust("lost partitions was detected");

                            return;
                        }

                        long setBaselineTimeout = baselineConfiguration.getBaselineAutoAdjustTimeout();

                        log.info("Baseline will be changed in '" + setBaselineTimeout + "' ms - ");

                        baselineAutoAdjustScheduler.schedule(lastBaselineData, setBaselineTimeout);
                    });

            }
        }
    }

    /**
     * Disable baseline auto adjust property in cluster.
     *
     * @param reason Reason of disable.
     */
    private void disableBaselineAutoAdjust(String reason) {
        log.warning("Baseline auto-adjust will be disable due to " + reason);

        try {
            baselineConfiguration.setBaselineAutoAdjustEnabled(false);
        }
        catch (IgniteCheckedException e) {
            log.error("Error during disable baseline auto-adjust", e);
        }
    }

    /**
     * @return {@code true} if auto-adjust baseline enabled.
     */
    private boolean isBaselineAutoAdjustEnabled() {
        return cluster.active() && baselineConfiguration.isBaselineAutoAdjustEnabled();
    }

    /**
     * @param first First argument.
     * @param second Second argument.
     * @return {@code true} if arguments are equal regardless on elements order.
     */
    public static boolean eqNotOrdered(Collection<BaselineNode> first, Collection<BaselineNode> second) {
        if (first == second)
            return true;

        if (first == null || second == null)
            return false;

        if (first.size() != second.size())
            return false;

        Set<Object> firstConsId = first.stream().map(BaselineNode::consistentId).collect(Collectors.toSet());

        return first.stream().map(BaselineNode::consistentId).allMatch(firstConsId::contains);
    }

    /**
     * @return Cluster coordinator, {@code null} if failed to determine.
     */
    @Nullable private ClusterNode coordinator() {
        return U.oldest(discoveryMgr.aliveServerNodes(), null);
    }

    /**
     * @return {@code true} if local node is coordinator.
     */
    private boolean isLocalNodeCoordinator() {
        DiscoverySpi spi = discoveryMgr.getInjectedDiscoverySpi();

        return spi instanceof TcpDiscoverySpi ?
            ((TcpDiscoverySpi)spi).isLocalNodeCoordinator() :
            F.eq(discoveryMgr.localNode(), coordinator());
    }
}
