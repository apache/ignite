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

package org.apache.ignite.internal.configuration.notifications;

import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration container for {@link ConfigurationNotificationEvent}.
 */
class ConfigurationContainer {
    /**
     * Configuration.
     *
     * <p>For {@link ConfigurationNotificationEvent#config} use {@link this#specificConfig()}.
     */
    final DynamicConfiguration<InnerNode, ?> config;

    /** Key in named list, for {@link ConfigurationNotificationEvent#name}. */
    @Nullable
    final String name;

    /** Previous container. */
    @Nullable
    final ConfigurationContainer prev;

    /**
     * Constructor.
     *
     * @param config Configuration.
     * @param name Key in named list.
     */
    ConfigurationContainer(
            DynamicConfiguration<InnerNode, ?> config,
            @Nullable String name,
            @Nullable ConfigurationContainer prev
    ) {
        this.config = config;
        this.name = name;
        this.prev = prev;
    }

    /**
     * Returns the configuration for {@link ConfigurationNotificationEvent#config}.
     */
    @Nullable ConfigurationProperty<InnerNode> specificConfig() {
        return config.isRemovedFromNamedList() ? null : config.specificConfigTree();
    }

    /**
     * Returns the configuration class.
     */
    Class<?> configClass() {
        Class<?> polymorphicInstanceConfigType = config.polymorphicInstanceConfigType();

        return polymorphicInstanceConfigType != null ? polymorphicInstanceConfigType : config.getClass();
    }
}
