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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_CONNECTION_FAILED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/**
 * Command line handler test with SSL.
 */
@RunWith(JUnit4.class)
public class GridCommandHandlerSslTest extends GridCommonAbstractTest {
    /** */
    private volatile String[] cipherSuites;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @return SSL factory.
     */
    @NotNull private SslContextFactory createSslFactory() {
        SslContextFactory factory = (SslContextFactory)GridTestUtils.sslFactory();

        factory.setCipherSuites(cipherSuites);

        return factory;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(100 * 1024 * 1024);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());
        cfg.getConnectorConfiguration().setSslEnabled(true);
        cfg.setSslContextFactory(createSslFactory());

        return cfg;
    }

    /**
     * @param nodeCipherSuite Ciphers suites to set on node.
     * @param utilityCipherSuite Ciphers suites to set on utility.
     * @param expRes Expected result.
     * @throws Exception If failed.
     */
    private void activate(String nodeCipherSuite, String utilityCipherSuite, int expRes) throws Exception {
        cipherSuites = F.isEmpty(nodeCipherSuite) ? null : nodeCipherSuite.split(",");

        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        final CommandHandler cmd = new CommandHandler();

        List<String> params = new ArrayList<>();
        params.add("--activate");
        params.add("--keystore");
        params.add(GridTestUtils.keyStorePath("node01"));
        params.add("--keystore-password");
        params.add(GridTestUtils.keyStorePassword());

        if (!F.isEmpty(utilityCipherSuite)) {
            params.add("--ssl-cipher-suites");
            params.add(utilityCipherSuite);
        }

        assertEquals(expRes, cmd.execute(params));

        if (expRes == EXIT_CODE_OK)
            assertTrue(ignite.cluster().active());
        else
            assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_CONNECTION_FAILED, cmd.execute(Arrays.asList("--deactivate", "--yes")));
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testDefaultCipherSuite() throws Exception {
        cipherSuites = null;

        activate(null, null, EXIT_CODE_OK);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSameCipherSuite() throws Exception {
        String ciphers = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256," +
            "TLS_RSA_WITH_AES_128_GCM_SHA256," +
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        activate(ciphers, ciphers, EXIT_CODE_OK);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneCommonCipherSuite() throws Exception {
        String nodeCipherSuites = "TLS_RSA_WITH_AES_128_GCM_SHA256," +
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        String utilityCipherSuites = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256," +
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        activate(nodeCipherSuites, utilityCipherSuites, EXIT_CODE_OK);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoCommonCipherSuite() throws Exception {
        String nodeCipherSuites = "TLS_RSA_WITH_AES_128_GCM_SHA256";

        String utilityCipherSuites = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256," +
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        activate(nodeCipherSuites, utilityCipherSuites, EXIT_CODE_CONNECTION_FAILED);
    }
}
