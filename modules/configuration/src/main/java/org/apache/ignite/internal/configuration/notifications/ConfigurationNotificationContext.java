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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Context to notify configuration listeners.
 */
class ConfigurationNotificationContext {
    /** Storage revision. */
    private final long storageRevision;

    /** The tail of containers, implements a stack for safe traversal in {@link ConfigurationNotificationEventImpl events}. */
    @Nullable
    private ConfigurationContainer tailContainers;

    /** For collect configuration listener futures. */
    final Collection<CompletableFuture<?>> futures = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param storageRevision Storage revision.
     */
    ConfigurationNotificationContext(long storageRevision) {
        this.storageRevision = storageRevision;
    }

    /**
     * Adds {@link ConfigurationContainer container}.
     *
     * @param config Configuration.
     * @param name Key in named list.
     */
    void addContainer(DynamicConfiguration<InnerNode, ?> config, @Nullable String name) {
        tailContainers = new ConfigurationContainer(config, name, tailContainers);
    }

    /**
     * Removes {@link ConfigurationContainer container}.
     *
     * @param config Configuration.
     */
    void removeContainer(DynamicConfiguration<InnerNode, ?> config) {
        assert tailContainers != null;
        assert tailContainers.config == config;

        tailContainers = tailContainers.prev;
    }

    /**
     * Creates an {@link ConfigurationNotificationEvent event}.
     *
     * @param <VIEWT> Type of the subtree or the value that has been changed.
     * @param oldValue Old value.
     * @param newValue New value.
     */
    <VIEWT> ConfigurationNotificationEvent<VIEWT> createEvent(@Nullable VIEWT oldValue, @Nullable VIEWT newValue) {
        assert tailContainers != null;

        return new ConfigurationNotificationEventImpl<>(oldValue, newValue, storageRevision, tailContainers);
    }
}
