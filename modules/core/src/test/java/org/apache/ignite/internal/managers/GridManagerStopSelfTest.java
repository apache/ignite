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

package org.apache.ignite.internal.managers;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.checkpoint.sharedfs.*;
import org.apache.ignite.spi.collision.*;
import org.apache.ignite.spi.collision.fifoqueue.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.deployment.*;
import org.apache.ignite.spi.deployment.local.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.eventstorage.*;
import org.apache.ignite.spi.eventstorage.memory.*;
import org.apache.ignite.spi.failover.always.*;
import org.apache.ignite.spi.loadbalancing.roundrobin.*;
import org.apache.ignite.spi.swapspace.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.internal.managers.checkpoint.*;
import org.apache.ignite.internal.managers.collision.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.managers.failover.*;
import org.apache.ignite.internal.managers.loadbalancer.*;
import org.apache.ignite.internal.managers.swapspace.*;
import org.apache.ignite.internal.processors.resource.*;
import org.apache.ignite.testframework.junits.*;
import org.apache.ignite.testframework.junits.common.*;

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
            IgniteLoggerResource.class,
            ctx.config().getGridLogger().getLogger(target.getClass())
        );
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testStopCommunicationManager() throws Exception {
        CommunicationSpi spi = new TcpCommunicationSpi();

        injectLogger(spi);

        ctx.config().setCommunicationSpi(spi);
        ctx.config().setMarshaller(new IgniteOptimizedMarshaller());

        GridIoManager mgr = new GridIoManager(ctx);

        mgr.onKernalStop(true);

        mgr.stop(false);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testStopLoadBalancingManager() throws Exception {
        RoundRobinLoadBalancingSpi spi = new RoundRobinLoadBalancingSpi();

        injectLogger(spi);

        ctx.config().setLoadBalancingSpi(spi);

        GridLoadBalancerManager mgr = new GridLoadBalancerManager(ctx);

        mgr.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopSwapSpaceManager() throws Exception {
        SwapSpaceSpi spi = new FileSwapSpaceSpi();

        injectLogger(spi);

        ctx.config().setSwapSpaceSpi(spi);

        GridSwapSpaceManager mgr = new GridSwapSpaceManager(ctx);

        mgr.stop(true);
    }
}
