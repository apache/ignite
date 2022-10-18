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

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.util.Collections;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 *
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentConfigSelfTest extends GridSpiAbstractConfigTest<UriDeploymentSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new UriDeploymentSpi(), "uriList", null);
        checkNegativeSpiProperty(new UriDeploymentSpi(), "uriList", Collections.singletonList("qwertyuiop"), false);
        checkNegativeSpiProperty(new UriDeploymentSpi(), "uriList", Collections.singletonList(null), false);
    }

    /**
     * Verifies that mixing LocalDeploymentSpi and UriDeploymentSpi doesn't violate configuration consistency rules.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientSpiConsistencyChecked() throws Exception {
        String srvName = "server";
        String clientName = "client";

        IgniteConfiguration srvCfg = getConfiguration();

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();
        String tmpDir = GridTestProperties.getProperty("deploy.uri.tmpdir");
        File tmp = new File(tmpDir);

        if (!tmp.exists())
            tmp.mkdir();

        deploymentSpi.setUriList(Collections.singletonList("file://" + tmpDir));
        srvCfg.setDeploymentSpi(deploymentSpi);

        try {
            startGrid(srvName, srvCfg);

            IgniteConfiguration clientCfg = getConfiguration();
            startClientGrid(clientName, clientCfg);
        }
        finally {
            stopAllGrids();
        }
    }
}
