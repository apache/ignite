package org.apache.ignite.plugin;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

public class NodeValidationPluginProvider implements PluginProvider, IgnitePlugin {

    private NodeValidationPluginConfiguration pluginConfiguration;
    public static volatile boolean enabled;

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
}
