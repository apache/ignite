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

    /**
     * The time interval in seconds for printing cache size information. 
     */
    private long interval = 10;

    private MyPlugin plugin;

    public MyPluginProvider() {
    }

    /**
     * 
     * @param interval Time interval in seconds
     */
    public MyPluginProvider(long interval) {
        this.interval = interval;
    }

    @Override
    public String name() {
        //the name of the plugin
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
        plugin = new MyPlugin(interval, ctx);
    }

    @Override
    public void onIgniteStart() throws IgniteCheckedException {
        //start the plugin when Ignite is started
        plugin.start();
    }

    @Override
    public void onIgniteStop(boolean cancel) {
        //stop the plugin
        plugin.stop();
    }

    /**
     * The time interval (in seconds) for printing cache size information 
     * @return 
     */
    public long getInterval() {
        return interval;
    }

    /**
     * Sets the time interval (in seconds) for printing cache size information
     * @param interval 
     */
    public void setInterval(long interval) {
        this.interval = interval;
    }

    // other no-op methods of PluginProvider 
    //tag::no-op-methods[]
    @Override
    public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
        System.out.println(cls);
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
