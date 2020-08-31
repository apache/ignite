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

package org.apache.ignite.platform.plugin;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.PlatformPluginExtension;
import org.apache.ignite.platform.plugin.cache.PlatformTestCachePluginProvider;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;

/**
 * Plugin provider for platform tests.
 */
public class PlatformTestPluginProvider extends AbstractTestPluginProvider {
    /** {@inheritDoc} */
    @Override public String name() {
        return "TestPlatformPlugin";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "-";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        registry.registerExtension(PlatformPluginExtension.class,
                new PlatformTestPluginExtension((IgniteEx) ctx.grid()));
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new PlatformTestPlugin();
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return new PlatformTestCachePluginProvider();
    }
}
