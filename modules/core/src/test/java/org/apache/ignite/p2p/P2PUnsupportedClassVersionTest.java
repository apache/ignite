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

package org.apache.ignite.p2p;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.p2p.SharedDeploymentTest.RUN_LAMBDA;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests user error (not server node failure) in case compute task compiled in unsupported bytecode version. */
public class P2PUnsupportedClassVersionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true)
            .setCommunicationSpi(new SendComputeWithHigherClassVersionTcpCommunicationSpi())
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** */
    @Test
    public void testCompute() throws Exception {
        try (Ignite server = startGrid("server"); Ignite client = startClientGrid("client")) {
            ClassLoader ldr = getExternalClassLoader();

            Class<?> lambdaFactoryCls = ldr.loadClass(RUN_LAMBDA);
            Method method = lambdaFactoryCls.getMethod("lambda");

            IgniteCallable<Integer> lambda = (IgniteCallable<Integer>)method.invoke(lambdaFactoryCls);

            assertThrowsWithCause(
                () -> client.compute(client.cluster().forServers()).broadcast(lambda),
                UnsupportedClassVersionError.class
            );

            client.createCache("Can_create_cache_after_compute_fail");
        }
    }

    /**
     * Custom communication SPI for simulating long running cache futures.
     */
    private static class SendComputeWithHigherClassVersionTcpCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        public SendComputeWithHigherClassVersionTcpCommunicationSpi() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDeploymentResponse) {
                GridDeploymentResponse resp = (GridDeploymentResponse)((GridIoMessage)msg).message();

                try {
                    Field byteSrcFld = resp.getClass().getDeclaredField("byteSrc");

                    byteSrcFld.setAccessible(true);

                    GridByteArrayList byteSrc = (GridByteArrayList)byteSrcFld.get(resp);

                    assertEquals(0xCAFEBABE, byteSrc.getInt(0));

                    // Assert minor version and first byte of major class version is zero.
                    assertEquals(0, byteSrc.get(4));
                    assertEquals(0, byteSrc.get(5));
                    assertEquals(0, byteSrc.get(6));

                    byte majorClassVersion = byteSrc.get(7);

                    assertTrue(byteSrc.get(7) > 0);

                    byteSrc.set(7, majorClassVersion + 1);
                }
                catch (IllegalAccessException | NoSuchFieldException e) {
                    throw new IgniteException(e);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
