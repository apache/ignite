/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManager;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.failover.GridFailoverManager;
import org.apache.ignite.internal.managers.loadbalancer.GridLoadBalancerManager;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.eventstorage.EventStorageSpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Managers stop test.
 *
 */
public class GridManagerStopSelfTest extends GridCommonAbstractTest {
    /** Kernal context. */
    private GridTestKernalContext ctx;

    /** */
    public GridManagerStopSelfTest() {
        super(/*startGrid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.config().setPeerClassLoadingEnabled(true);

        ctx.add(new PoolProcessor(ctx));
        ctx.add(new GridResourceProcessor(ctx));

        ctx.start();
    }

    /**
     * @param target Target spi.
     * @throws IgniteCheckedException If injection failed.
     */
    private void injectLogger(IgniteSpi target) throws IgniteCheckedException {
        ctx.resource().injectBasicResource(
            target,
            LoggerResource.class,
            ctx.config().getGridLogger().getLogger(target.getClass())
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopCheckpointManager() throws Exception {
        SharedFsCheckpointSpi spi = new SharedFsCheckpointSpi();

        injectLogger(spi);

        ctx.config().setCheckpointSpi(spi);

        GridCheckpointManager mgr = new GridCheckpointManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopCollisionManager() throws Exception {
        CollisionSpi spi = new FifoQueueCollisionSpi();

        injectLogger(spi);

        ctx.config().setCollisionSpi(spi);

        GridCollisionManager mgr = new GridCollisionManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopCommunicationManager() throws Exception {
        CommunicationSpi spi = new TcpCommunicationSpi();

        injectLogger(spi);

        ctx.config().setCommunicationSpi(spi);
        ctx.config().setMarshaller(new BinaryMarshaller());
        ctx.config().setMetricExporterSpi(new NoopMetricExporterSpi());
        ctx.add(new GridMetricManager(ctx));

        GridIoManager mgr = new GridIoManager(ctx);

        mgr.onKernalStop(true);

        mgr.stop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopDeploymentManager() throws Exception {
        DeploymentSpi spi = new LocalDeploymentSpi();

        injectLogger(spi);

        ctx.config().setDeploymentSpi(spi);

        GridDeploymentManager mgr = new GridDeploymentManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopDiscoveryManager() throws Exception {
        DiscoverySpi spi = new TcpDiscoverySpi();

        injectLogger(spi);

        ctx.config().setDiscoverySpi(spi);

        GridDiscoveryManager mgr = new GridDiscoveryManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopEventStorageManager() throws Exception {
        EventStorageSpi spi = new MemoryEventStorageSpi();

        injectLogger(spi);

        ctx.config().setEventStorageSpi(spi);

        GridEventStorageManager mgr = new GridEventStorageManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopFailoverManager() throws Exception {
        AlwaysFailoverSpi spi = new AlwaysFailoverSpi();

        injectLogger(spi);

        ctx.config().setFailoverSpi(spi);

        GridFailoverManager mgr = new GridFailoverManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopLoadBalancingManager() throws Exception {
        RoundRobinLoadBalancingSpi spi = new RoundRobinLoadBalancingSpi();

        injectLogger(spi);

        ctx.config().setLoadBalancingSpi(spi);

        GridLoadBalancerManager mgr = new GridLoadBalancerManager(ctx);

        mgr.stop(true);
    }
}
