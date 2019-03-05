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

package org.apache.ignite.client;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Tests cases when node connects to cluster with different set of cipher suites.
 */
public class SslParametersTest extends GridCommonAbstractTest {

    public static final String TEST_CACHE_NAME = "TEST";
    /** */
    private volatile String[] cipherSuites;

    /** */
    private volatile String[] protocols;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setUseIgniteSslContextFactory(true));

        cfg.setSslContextFactory(createSslFactory());

        CacheConfiguration ccfg = new CacheConfiguration(TEST_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    protected ClientConfiguration getClientConfiguration() throws Exception {
        ClientConfiguration cfg = new ClientConfiguration();

        cfg.setAddresses("127.0.0.1:10800");

        cfg.setSslMode(SslMode.REQUIRED);

        cfg.setSslContextFactory(createSslFactory());

        return cfg;
    }

    @NotNull private SslContextFactory createSslFactory() {
        SslContextFactory factory = (SslContextFactory)GridTestUtils.sslTrustedFactory(
            "node01", "trustone");

        factory.setCipherSuites(cipherSuites);
        factory.setProtocols(protocols);

        return factory;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        protocols = null;
        cipherSuites = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameCipherSuite() throws Exception {
        cipherSuites = new String[] {
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
        };

        startGrid();

        checkSuccessfulClientStart(
            new String[][] {
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
    public void testOneCommonCipherSuite() throws Exception {
        cipherSuites = new String[] {
            "TLS_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
        };

        startGrid();
        
        checkSuccessfulClientStart(
            new String[][] {
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
    public void testNoCommonCipherSuite() throws Exception {
        cipherSuites = new String[] {
            "TLS_RSA_WITH_AES_128_GCM_SHA256"
        };

        startGrid();
        
        checkClientStartFailure(
            new String[][] {
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
    public void testNonExistentCipherSuite() throws Exception {
        cipherSuites = new String[] {
            "TLS_RSA_WITH_AES_128_GCM_SHA256"
        };

        startGrid();
        
        checkClientStartFailure(
            new String[][] {
                new String[] {
                    "TLC_FAKE_CIPHER",
                    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                }
            },
            null,
            IllegalArgumentException.class,
            "Unsupported ciphersuite"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoCommonProtocols() throws Exception {
        protocols = new String[] {
            "TLSv1.1",
            "SSLv3"
        };

        startGrid();

        checkClientStartFailure(
            null,
            new String[][] {
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
    public void testNonExistentProtocol() throws Exception {
        protocols = new String[] {
            "SSLv3"
        };

        startGrid();

        checkClientStartFailure(
            null,
            new String[][] {
                new String[] {
                    "SSLv3",
                    "SSLvDoesNotExist"
                }
            },
            IllegalArgumentException.class,
            "SSLvDoesNotExist"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameProtocols() throws Exception {
        protocols = new String[] {
            "TLSv1.1",
            "TLSv1.2",
        };

        startGrid();

        checkSuccessfulClientStart(null,
            new String[][] {
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
    public void testOneCommonProtocol() throws Exception {
        protocols = new String[] {
            "TLSv1",
            "TLSv1.1",
            "TLSv1.2"
        };

        startGrid();

        checkSuccessfulClientStart(null,
            new String[][] {
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
    private void checkSuccessfulClientStart(String[][] cipherSuites, String[][] protocols) throws Exception {
        int n = Math.max(
            cipherSuites != null ? cipherSuites.length : 0,
            protocols != null ? protocols.length : 0);

        for (int i = 0; i < n; i++) {
            this.cipherSuites = cipherSuites != null && i < cipherSuites.length ? cipherSuites[i] : null;
            this.protocols = protocols != null && i < protocols.length ? protocols[i] : null;

            IgniteClient client = Ignition.startClient(getClientConfiguration());

            client.getOrCreateCache(TEST_CACHE_NAME);

            client.close();
        }
    }

    /**
     * @param cipherSuites list of cipher suites
     * @param protocols list of protocols
     * @throws Exception If failed.
     */
    private void checkClientStartFailure(String[][] cipherSuites, String[][] protocols) throws Exception {
        checkClientStartFailure(cipherSuites, protocols, ClientConnectionException.class, "Ignite cluster is unavailable");
    }

    /**
     * @param cipherSuites list of cipher suites
     * @param protocols list of protocols
     * @param ex expected exception class
     * @param msg exception message
     * @throws Exception If failed.
     */
    private void checkClientStartFailure(String[][] cipherSuites, String[][] protocols, Class<? extends Throwable> ex, String msg) throws Exception {
        int n = Math.max(
            cipherSuites != null ? cipherSuites.length : 0,
            protocols != null ? protocols.length : 0);

        for (int i = 0; i < n; i++) {
            this.cipherSuites = cipherSuites != null && i < cipherSuites.length ? cipherSuites[i] : null;
            this.protocols = protocols != null && i < protocols.length ? protocols[i] : null;

            int finalI = i;

            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Ignition.startClient(getClientConfiguration());

                    return null;
                }
            }, ex, msg);
        }
    }

}
