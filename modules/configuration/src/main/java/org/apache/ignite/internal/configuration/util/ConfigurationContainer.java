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

package org.apache.ignite.internal.configuration.util;

import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration container for {@link ConfigurationNotificationEvent#config} and {@link ConfigurationNotificationEvent#name}.
 */
class ConfigurationContainer {
    /** Key of the named list item for {@link ConfigurationNotificationEvent#name}. */
    @Nullable
    final String keyNamedConfig;

    /** Configuration for {@link ConfigurationNotificationEvent#config}. */
    @Nullable
    final ConfigurationProperty<InnerNode> config;

    /**
     * Constructor.
     *
     * @param keyNamedConfig Key of the named list item for {@link ConfigurationNotificationEvent#name}.
     * @param config         Configuration for {@link ConfigurationNotificationEvent#config}.
     */
    ConfigurationContainer(
            @Nullable String keyNamedConfig,
            @Nullable ConfigurationProperty<InnerNode> config
    ) {
        this.keyNamedConfig = keyNamedConfig;
        this.config = config;
    }
}
