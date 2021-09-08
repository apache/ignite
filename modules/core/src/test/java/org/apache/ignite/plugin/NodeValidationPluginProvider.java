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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Validates node on join, it requires nodes to provide token that matches configured on primary node.
 */
public class NodeValidationPluginProvider extends AbstractTestPluginProvider implements IgnitePlugin {
    /** */
    private NodeValidationPluginConfiguration pluginConfiguration;

    /** */
    private static volatile boolean enabled;

    /** */
    public static boolean isEnabled() {
        return enabled;
    }

    /** */
    public static void setEnabled(boolean enabled) {
        NodeValidationPluginProvider.enabled = enabled;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "NodeValidationPluginProvider";
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        if (!enabled)
            return;

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

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        if (!enabled)
            return null;

        MyDiscoData data = new MyDiscoData(pluginConfiguration.getToken());

        return data;
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node, Serializable serializable) {
        if (!enabled)
            return;

        MyDiscoData newNodeDiscoData = serializable instanceof MyDiscoData ? (MyDiscoData)serializable : null;

        if (newNodeDiscoData == null || !newNodeDiscoData.getToken().equals(pluginConfiguration.getToken())) {
            String msg = newNodeDiscoData == null ? "no token provided" : "bad token provided: " + newNodeDiscoData.getToken();

            throw new PluginValidationException(msg, msg, node.id());
        }
    }

    /**
     *
     */
    private static class MyDiscoData implements Serializable {
        /**  */
        String token;

        /**  */
        MyDiscoData(String token) {
            this.token = token;
        }

        /**  */
        public String getToken() {
            return token;
        }

        /**  */
        @Override public String toString() {
            return "MyDiscoData{" +
                "token='" + token + '\'' +
                '}';
        }
    }

    /**
     *
     */
    public static class NodeValidationPluginConfiguration implements PluginConfiguration {
        /** */
        private final String token;

        /** */
        NodeValidationPluginConfiguration(String token) {
            this.token = token;
        }

        /** */
        public String getToken() {
            return token;
        }
    }
}
