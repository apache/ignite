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

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cases when node connects to cluster with different SSL configuration.
 * Exception with meaningful message should be thrown.
 */
public class TcpDiscoverySslTrustedUntrustedTest extends GridCommonAbstractTest {
    /** */
    private volatile String keyStore;

    /** */
    private volatile String trustStore;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSslContextFactory(GridTestUtils.sslTrustedFactory(keyStore, trustStore));

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
    public void testSameKey() throws Exception {
        checkDiscoverySuccess("node01", "trustone", "node01", "trustone");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDifferentKeys() throws Exception {
        checkDiscoverySuccess("node02", "trusttwo", "node03", "trusttwo");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBothTrusts() throws Exception {
        checkDiscoverySuccess("node01", "trustboth", "node02", "trustboth", "node03", "trustboth");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTrustOneMultiCert() throws Exception {
        checkDiscoverySuccess("node01", "trustone", "node0102", "trustone");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTrustBothMultiCert() throws Exception {
        checkDiscoverySuccess("node03", "trustboth", "node0102", "trusttwo");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDifferentCa() throws Exception {
        checkDiscoveryFailure("node01", "trustone", "node02", "trusttwo");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongCa() throws Exception {
        checkDiscoveryFailure("node02", "trustone", "node03", "trustone");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMismatchingCaSecond() throws Exception {
        checkDiscoveryFailure("node01", "trustboth", "node03", "trusttwo");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMismatchingCaFirst() throws Exception {
        checkDiscoveryFailure("node02", "trusttwo", "node01", "trustboth");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpired() throws Exception {
        checkDiscoveryFailure("node02old", "trusttwo", "node03", "trusttwo");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpiredMismatchingCa() throws Exception {
        checkDiscoveryFailure("node01", "trustboth", "node02old", "trusttwo");
    }

    /**
     * @param keysTrusts Pairs of key store, trust store.
     * @throws Exception If failed.
     */
    private void checkDiscoverySuccess(String... keysTrusts) throws Exception {
        if (keysTrusts.length % 2 != 0)
            fail("Wrong parameters");

        for (int i = 0; i < keysTrusts.length / 2; i++) {
            keyStore = keysTrusts[2 * i];
            trustStore = keysTrusts[2 * i + 1];

            startGrid(i);
        }
    }

    /**
     * @param keyStoreOk Key store of first instance.
     * @param trustStoreOk Trust store of first instance.
     * @param keyStoreFail Key store of second (failing) instance.
     * @param trustStoreFail Trust store of second (failing) instance.
     * @throws Exception If failed.
     */
    private void checkDiscoveryFailure(String keyStoreOk, String trustStoreOk,
        final String keyStoreFail, final String trustStoreFail) throws Exception {
        keyStore = keyStoreOk;
        trustStore = trustStoreOk;

        startGrid(0);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                keyStore = keyStoreFail;
                trustStore = trustStoreFail;

                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, "Unable to establish secure connection.");
    }
}
