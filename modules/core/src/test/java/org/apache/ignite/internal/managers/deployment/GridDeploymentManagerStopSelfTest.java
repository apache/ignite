/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.managers.deployment;

import java.util.Map;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Grid deployment manager stop test.
 */
@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
public class GridDeploymentManagerStopSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOnKernalStop() throws Exception {
        DeploymentSpi spi = new GridTestDeploymentSpi();

        GridTestKernalContext ctx = newContext();

        ctx.config().setMarshaller(new JdkMarshaller());
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
        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException { /* No-op. */ }

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

        /** {@inheritDoc} */
        @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) { /* No-op. */ }

        /** {@inheritDoc} */
        @Override public void onClientReconnected(boolean clusterRestarted) { /* No-op. */ }
    }
}
