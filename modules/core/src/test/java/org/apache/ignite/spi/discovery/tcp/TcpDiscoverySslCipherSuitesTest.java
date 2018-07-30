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
import javax.net.ssl.SSLParameters;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests cases when node connects to cluster with different set of cipher suites.
 */
public class TcpDiscoverySslCipherSuitesTest extends GridCommonAbstractTest {

    /** */
    private volatile String[] cipherSuites;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        SslContextFactory factory = (SslContextFactory)GridTestUtils.sslTrustedFactory("node01", "trustone");

        factory.setCipherSuites(cipherSuites);

        cfg.setSslContextFactory(factory);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameCipherSuites() throws Exception {
        checkDiscoverySuccess(
            new String[][] {
                new String[] {
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                },
                new String[] {
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneEqualCipherSuite() throws Exception {
        checkDiscoverySuccess(
            new String[][] {
                new String[] {
                    "TLS_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                },
                new String[] {
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoCommonCipherSuite() throws Exception {
        checkDiscoveryFailure(
            new String[][] {
                new String[] {
                    "TLS_RSA_WITH_AES_128_GCM_SHA256",
                },
                new String[] {
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                }
            }
        );
    }

    /**
     * @param cipherSuites list of cipher suites
     * @throws Exception If failed.
     */
    private void checkDiscoverySuccess(String[][] cipherSuites) throws Exception {
        for (int i = 0; i < cipherSuites.length; i++) {
            this.cipherSuites = cipherSuites[i];

            startGrid(i);
        }
    }

    /**
     * @param cipherSuites list of cipher suites
     * @throws Exception If failed.
     */
    private void checkDiscoveryFailure(String[][] cipherSuites) throws Exception {
        this.cipherSuites = cipherSuites[0];

        startGrid(0);

        for (int i = 1; i < cipherSuites.length; i++) {
            this.cipherSuites = cipherSuites[i];

            int finalI = i;

            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(finalI);

                    return null;
                }
            }, IgniteCheckedException.class, "Unable to establish secure connection.");
        }
    }

}
