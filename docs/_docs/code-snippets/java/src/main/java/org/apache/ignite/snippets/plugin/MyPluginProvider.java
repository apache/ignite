package org.apache.ignite.snippets.plugin;

import java.io.Serializable;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

public class MyPluginProvider implements PluginProvider<PluginConfiguration> {

    private long period = 10;

    private MyPlugin plugin;

    public MyPluginProvider() {
    }

    /**
     * 
     * @param period period in seconds
     */
    public MyPluginProvider(long period) {
        this.period = period;
    }

    @Override
    public String name() {
        return "MyPlugin";
    }

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public String copyright() {
        return "MyCompany";
    }

    @Override
    public MyPlugin plugin() {
        return plugin;
    }

    @Override
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry)
            throws IgniteCheckedException {
        plugin = new MyPlugin(period, ctx);
    }

    @Override
    public void onIgniteStart() throws IgniteCheckedException {
        plugin.start();
    }

    @Override
    public void onIgniteStop(boolean cancel) {
        plugin.stop();
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    // other no-op methods of PluginProvider 
    //tag::no-op-methods[]
    @Override
    public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    @Override
    public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    @Override
    public void start(PluginContext ctx) throws IgniteCheckedException {
    }

    @Override
    public void stop(boolean cancel) throws IgniteCheckedException {
    }

    @Override
    public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override
    public void receiveDiscoveryData(UUID nodeId, Serializable data) {
    }

    @Override
    public void validateNewNode(ClusterNode node) throws PluginValidationException {
    }
    //end::no-op-methods[]
}
