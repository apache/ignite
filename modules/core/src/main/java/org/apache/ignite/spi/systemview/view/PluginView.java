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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.systemview.Filtrable;
import org.apache.ignite.internal.systemview.Order;
import org.apache.ignite.internal.systemview.SystemViewDescriptor;
import org.apache.ignite.plugin.PluginProvider;

/** Ignite plugin representation for a {@link SystemView}. */
@SystemViewDescriptor
public class PluginView {
    /** */
    private final PluginProvider<?> pluginProvider;

    /**
     * Constructor.
     *
     * @param pluginProvider Plugin provider.
     */
    public PluginView(PluginProvider<?> pluginProvider) {
        this.pluginProvider = pluginProvider;
    }

    /** @return Plugin name. */
    @Order
    @Filtrable
    public String name() {
        return pluginProvider.name();
    }

    /** @return Plugin version. */
    @Order(1)
    @Filtrable
    public String version() {
        return pluginProvider.version();
    }

    /** @return Plugin class name. */
    @Order(2)
    @Filtrable
    public String className() {
        return pluginProvider.plugin().getClass().getName();
    }

    /** @return Plugin info. */
    @Order(3)
    @Filtrable
    public String info() {
        return pluginProvider.info();
    }
}
