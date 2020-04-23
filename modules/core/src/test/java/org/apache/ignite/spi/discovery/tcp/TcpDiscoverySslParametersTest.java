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
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cases when node connects to cluster with different set of cipher suites.
 */
public class TcpDiscoverySslParametersTest extends GridCommonAbstractTest {

    /** */
    private volatile String[] cipherSuites;

    /** */
    private volatile String[] protocols;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        SslContextFactory factory = (SslContextFactory)GridTestUtils.sslTrustedFactory(
            "node01", "trustone");

        factory.setCipherSuites(cipherSuites);

        factory.setProtocols(protocols);

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
    @Test
    public void testSameCipherSuite() throws Exception {
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
            },
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneCommonCipherSuite() throws Exception {
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
            },
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
            },
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonExistentCipherSuite() throws Exception {
        checkDiscoveryFailure(
            new String[][] {
                new String[] {
                    "TLS_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                },
                new String[] {
                    "TLC_FAKE_CIPHER",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                }
            },
            null,
            IgniteCheckedException.class,
            // Java 8 has "Unsupported ciphersuite", Java 11 has "Unsupported CipherSuite"
            "Unsupported"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoCommonProtocols() throws Exception {
        checkDiscoveryFailure(
            null,
            new String[][] {
                new String[] {
                    "TLSv1.1",
                    "SSLv3"
                },
                new String[] {
                    "TLSv1",
                    "TLSv1.2",
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonExistentProtocol() throws Exception {
        checkDiscoveryFailure(
            null,
            new String[][] {
                new String[] {
                    "SSLv3"
                },
                new String[] {
                    "SSLv3",
                    "SSLvDoesNotExist"
                }
            },
            IgniteCheckedException.class,
            "SSLvDoesNotExist"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSameProtocols() throws Exception {
        checkDiscoverySuccess(null,
            new String[][] {
                new String[] {
                    "TLSv1.1",
                    "TLSv1.2",
                },
                new String[] {
                    "TLSv1.1",
                    "TLSv1.2",
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneCommonProtocol() throws Exception {
        checkDiscoverySuccess(null,
            new String[][] {
                new String[] {
                    "TLSv1",
                    "TLSv1.1",
                    "TLSv1.2",
                },
                new String[] {
                    "TLSv1.1",
                    "SSLv3"
                }
            }
        );
    }

    /**
     * @param cipherSuites list of cipher suites
     * @param protocols list of protocols
     * @throws Exception If failed.
     */
    private void checkDiscoverySuccess(String[][] cipherSuites, String[][] protocols) throws Exception {
        int n = Math.max(
            cipherSuites != null ? cipherSuites.length : 0,
            protocols != null ? protocols.length : 0);

        for (int i = 0; i < n; i++) {
            this.cipherSuites = cipherSuites != null && i < cipherSuites.length ? cipherSuites[i] : null;
            this.protocols = protocols != null && i < protocols.length ? protocols[i] : null;

            startGrid(i);
        }
    }

    /**
     * @param cipherSuites list of cipher suites
     * @param protocols list of protocols
     * @throws Exception If failed.
     */
    private void checkDiscoveryFailure(String[][] cipherSuites, String[][] protocols) throws Exception {
        checkDiscoveryFailure(cipherSuites, protocols, IgniteCheckedException.class, "Unable to establish secure connection.");
    }

    /**
     * @param cipherSuites list of cipher suites
     * @param protocols list of protocols
     * @param ex expected exception class
     * @param msg exception message
     * @throws Exception If failed.
     */
    private void checkDiscoveryFailure(String[][] cipherSuites, String[][] protocols, Class<? extends Throwable> ex, String msg) throws Exception {
        this.cipherSuites = cipherSuites != null ? cipherSuites[0] : null;
        this.protocols = protocols != null ? protocols[0] : null;

        startGrid(0);

        int n = Math.max(
            cipherSuites != null ? cipherSuites.length : 0,
            protocols != null ? protocols.length : 0);

        for (int i = 1; i < n; i++) {
            this.cipherSuites = cipherSuites != null && i < cipherSuites.length ? cipherSuites[i] : null;
            this.protocols = protocols != null && i < protocols.length ? protocols[i] : null;

            int finalI = i;

            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(finalI);

                    return null;
                }
            }, ex, msg);
        }
    }

}
