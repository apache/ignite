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

/** Information for plugin needed for its creation and usage. */
public class IgnitePluginInfo {
    /** Plugin provider. */
    private PluginProvider provider;

    /** Plugin configuration. */
    private PluginConfiguration cfg;

    /** */
    public IgnitePluginInfo(PluginProvider provider) {
        this.provider = provider;
    }

    /**
     * Gets plugin provider.
     *
     * @return Plugin provider.
     */
    public PluginProvider getPluginProvider() {
        return provider;
    }

    /**
     * Gets plugin configuration.
     *
     * @return Plugin configuration.
     */
    public PluginConfiguration getPluginConfiguration() {
        return cfg;
    }

    /**
     * Sets plugin configuration.
     *
     * @param cfg Plugin configuration.
     * @return {@code this} for chaining.
     */
    public IgnitePluginInfo setPluginConfiguration(PluginConfiguration cfg) {
        this.cfg = cfg;

        return this;
    }
}
