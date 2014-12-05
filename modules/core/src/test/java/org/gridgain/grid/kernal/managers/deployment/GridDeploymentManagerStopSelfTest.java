/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.deployment;

import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.spi.deployment.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Grid deployment manager stop test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridDeploymentManagerStopSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testOnKernalStop() throws Exception {
        DeploymentSpi spi = new GridTestDeploymentSpi();

        GridTestKernalContext ctx = newContext();

        ctx.config().setMarshaller(new IgniteJdkMarshaller());
        ctx.config().setDeploymentSpi(spi);

        GridResourceProcessor resProc = new GridResourceProcessor(ctx);
        resProc.setSpringContext(null);

        ctx.add(resProc);

        GridComponent mgr = new GridDeploymentManager(ctx);

        try {
            mgr.onKernalStop(true);
        }
        catch (Exception e) {
            error("Error during onKernalStop() callback.", e);

            assert false : "Unexpected exception " + e;
        }
    }

    /**
     * Test deployment SPI implementation.
     */
    private static class GridTestDeploymentSpi implements DeploymentSpi {
        /** {@inheritDoc} */
        @Override public void onContextDestroyed() {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException { return null; }

        /** {@inheritDoc} */
        @Override public void onContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException { /* No-op. */ }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException { /* No-op. */ }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException { /* No-op. */ }

        /** {@inheritDoc} */
        @Override public void setListener(DeploymentListener lsnr) { /* No-op. */ }

        /** {@inheritDoc} */
        @Override public String getName() { return getClass().getSimpleName(); }

        /** {@inheritDoc} */
        @Override public DeploymentResource findResource(String rsrcName) { return null; }

        /** {@inheritDoc} */
        @Override public boolean register(ClassLoader ldr, Class<?> rsrc) throws IgniteSpiException { return false; }

        /** {@inheritDoc} */
        @Override public boolean unregister(String rsrcName) { return false; }
    }
}
