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
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the {@link ConfigurationNotificationEvent}.
 *
 * @param <VIEWT> Type of the subtree or the value that has been changed.
 */
class ConfigurationNotificationEventImpl<VIEWT> implements ConfigurationNotificationEvent<VIEWT> {
    /** Previous value of the updated configuration. */
    @Nullable
    private final VIEWT oldValue;

    /** Updated value of the configuration. */
    @Nullable
    private final VIEWT newValue;

    /** Storage revision. */
    private final long storageRevision;

    /** The tail of containers, implements a stack for safe traversal. */
    private final ConfigurationContainer tail;

    /**
     * Constructor.
     *
     * @param oldValue Old value.
     * @param newValue New value.
     * @param storageRevision Storage revision.
     * @param tail The tail of containers.
     */
    ConfigurationNotificationEventImpl(
            @Nullable VIEWT oldValue,
            @Nullable VIEWT newValue,
            long storageRevision,
            ConfigurationContainer tail
    ) {
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.storageRevision = storageRevision;
        this.tail = tail;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable VIEWT oldValue() {
        return oldValue;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable VIEWT newValue() {
        return newValue;
    }

    /** {@inheritDoc} */
    @Override
    public long storageRevision() {
        return storageRevision;
    }

    /** {@inheritDoc} */
    @Override
    public <T extends ConfigurationProperty> @Nullable T config(Class<?> configClass) {
        ConfigurationContainer container = findContainer(configClass);

        return container == null ? null : (T) container.specificConfig();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String name(Class<?> configClass) {
        ConfigurationContainer container = findContainer(configClass);

        return container == null ? null : container.name;
    }

    private @Nullable ConfigurationContainer findContainer(Class<?> configClass) {
        ConfigurationContainer curr = tail;

        while (curr != null) {
            if (configClass.isAssignableFrom(curr.configClass())) {
                return curr;
            } else {
                curr = curr.prev;
            }
        }

        return null;
    }
}
