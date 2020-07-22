package org.apache.ignite.snippets.plugin;

import java.io.Serializable;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridPluginComponent;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

public class MyPluginProvider implements PluginProvider<PluginConfiguration>{
    

    @Override
    public String name() {
        return "MyPlugin";
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public String copyright() {
        return null;
    }

    @Override
    public <T extends IgnitePlugin> T plugin() {
        System.out.println("plugin");
        return (T) new MyPlugin();
    }

    @Override
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        
    }

    @Override
    public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
        //return (T) new GridPluginComponent(this);
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
    public void onIgniteStart() throws IgniteCheckedException {
        
       System.out.println("onIgnitestart"); 
        
    }

    @Override
    public void onIgniteStop(boolean cancel) {
        
    }

    @Override
    public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // TODO Auto-generated method stub
        
    }

}
