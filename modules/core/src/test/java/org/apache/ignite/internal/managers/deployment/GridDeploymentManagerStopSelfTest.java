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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.apache.ignite.spi.deployment.*;
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
