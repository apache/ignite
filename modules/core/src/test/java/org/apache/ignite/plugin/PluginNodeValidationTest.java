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

package org.apache.ignite.plugin;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PluginNodeValidationTest extends GridCommonAbstractTest {

    private volatile String token;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        cfg.setConsistentId(igniteInstanceName);

        cfg.setPluginConfigurations(new NodeValidationPluginConfiguration(token));

        return cfg;
    }

    @Test
    public void testValidationException() throws Exception {
        token = "123456";

        startGrid(0);

        token = "abcdef";

        try {
            startGrid(1);
        } catch (Exception ex) {
            assertTrue("Wrong exception type for validation error", X.hasCause(ex, IgniteSpiException.class));

            return;
        }

        fail("Exception is expected due validation error in plugin");
    }

    @Test
    public void testSuccessfulValidation() throws Exception {
        token = "123456";

        startGrid(0);
        startGrid(1);
    }

    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    @BeforeClass
    public static void enablePlugin() {
        NodeValidationPluginProvider.enabled = true;
    }

    @AfterClass
    public static void disablePlugin() {
        NodeValidationPluginProvider.enabled = false;
    }
}
