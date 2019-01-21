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
    public static volatile boolean enabled;

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

    public static class NodeValidationPluginProvider implements PluginProvider, IgnitePlugin {

        private NodeValidationPluginConfiguration pluginConfiguration;

        @Override public String name() {
            return "NodeValidationPluginProvider";
        }

        @Override public String version() {
            return "1.0";
        }

        @Override public String copyright() {
            return "";
        }

        @Override public IgnitePlugin plugin() {
            return this;
        }

        @Override
        public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
            if(!enabled) return;

            IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

            if (igniteCfg.getPluginConfigurations() != null) {
                for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                    if (pluginCfg instanceof NodeValidationPluginConfiguration) {
                        pluginConfiguration = (NodeValidationPluginConfiguration)pluginCfg;

                        break;
                    }
                }
            }
        }

        @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
            return null;
        }

        @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
            return null;
        }

        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
            //no-op
        }

        @Override public void stop(boolean cancel) throws IgniteCheckedException {
            //no-op
        }

        @Override public void onIgniteStart() throws IgniteCheckedException {
            //no-op
        }

        @Override public void onIgniteStop(boolean cancel) {
            //no-op
        }

        @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
            if(!enabled) return null;

            MyDiscoData data = new MyDiscoData(pluginConfiguration.getToken());

            return data;
        }

        @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
            if(!enabled) return;
        }

        @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
            // no-op
        }

        @Override public void validateNewNode(ClusterNode node, Serializable serializable) {
            if(!enabled) return;

            MyDiscoData newNodeDiscoData = serializable instanceof MyDiscoData ? (MyDiscoData)serializable : null;

            if (newNodeDiscoData == null || !newNodeDiscoData.getToken().equals(pluginConfiguration.getToken())) {
                String msg = newNodeDiscoData == null ? "no token provided" : "bad token provided: " + newNodeDiscoData.getToken();

                throw new PluginValidationException(msg, msg, node.id());
            }
        }
    }

    public static class NodeValidationPluginConfiguration implements PluginConfiguration {
        private final String token;

        public NodeValidationPluginConfiguration(String token) {
            this.token = token;
        }

        public String getToken() {
            return token;
        }
    }

    private static class MyDiscoData implements Serializable {
        String token;

        public MyDiscoData(String token) {
            this.token = token;
        }

        public String getToken() {
            return token;
        }

        @Override public String toString() {
            return "MyDiscoData{" +
                "token='" + token + '\'' +
                '}';
        }
    }

    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    @BeforeClass
    public static void enablePlugin() {
        enabled = true;
    }

    @AfterClass
    public static void disablePlugin() {
        enabled = false;
    }
}
