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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}.
 */
public class TcpClientDiscoveryMarshallerCheckSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean testFooter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (testFooter) {
            cfg.setMarshaller(new BinaryMarshaller());

            TcpDiscoverySpi spi = new TcpDiscoverySpi();

            spi.setJoinTimeout(-1); // IGNITE-605, and further tests limitation bypass

            cfg.setDiscoverySpi(spi);

            if (igniteInstanceName.endsWith("0")) {
                BinaryConfiguration bc = new BinaryConfiguration();
                bc.setCompactFooter(false);

                cfg.setBinaryConfiguration(bc);
                cfg.setClientMode(true);
            }
        }
        else {
            if (igniteInstanceName.endsWith("0"))
                cfg.setMarshaller(new JdkMarshaller());
            else {
                cfg.setClientMode(true);
                cfg.setMarshaller(new BinaryMarshaller());
            }
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMarshallerInConsistency() throws Exception {
        startGrid(0);

        try {
            startGrid(1);

            fail("Expected SPI exception was not thrown.");
        }
        catch (IgniteCheckedException e) {
            Throwable ex = e.getCause().getCause();

            assertTrue(ex instanceof IgniteSpiException);
            assertTrue(ex.getMessage().contains("Local node's marshaller differs from remote node's marshaller"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInconsistentCompactFooterSingle() throws Exception {
        clientServerInconsistentConfigFail(false, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInconsistentCompactFooterMulti() throws Exception {
        clientServerInconsistentConfigFail(true, 2, 10);
    }

    /**
     * Starts client-server grid with different binary configurations.
     *
     * @throws Exception If failed.
     */
    private void clientServerInconsistentConfigFail(boolean multiNodes, int cnt, int iters) throws Exception {
        testFooter = true;

        for (int i = 1; i <= cnt; i++)
            startGrid(i);

        for (int i = 0; i < iters; i++) {
            try {
                startGrid(0);

                fail("Expected SPI exception was not thrown, multiNodes=" + multiNodes);
            }
            catch (IgniteCheckedException expect) {
                Throwable ex = expect.getCause().getCause();

                String msg = ex.getMessage();

                assertTrue(ex instanceof IgniteSpiException);
                assertTrue("Caught exception: " + msg, msg.contains("Local node's binary " +
                    "configuration is not equal to remote node's binary configuration"));
            }
            finally {
                stopGrid(0);
            }
        }
    }
}
