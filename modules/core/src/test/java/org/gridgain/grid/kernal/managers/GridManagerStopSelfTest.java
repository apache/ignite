/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers;

import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.checkpoint.*;
import org.gridgain.grid.kernal.managers.collision.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.managers.failover.*;
import org.gridgain.grid.kernal.managers.loadbalancer.*;
import org.gridgain.grid.kernal.managers.swapspace.*;
import org.gridgain.grid.kernal.processors.resource.*;
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
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

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
     * @throws GridException If injection failed.
     */
    private void injectLogger(IgniteSpi target) throws GridException {
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
