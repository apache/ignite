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

package org.apache.ignite.configuration.notifications;

import org.apache.ignite.configuration.ConfigurationProperty;
import org.jetbrains.annotations.Nullable;

/**
 * Event object propagated on configuration change. Passed to listeners after configuration changes are applied.
 *
 * @param <VIEWT> Type of the subtree or the value that has been changed.
 * @see ConfigurationProperty#listen(ConfigurationListener)
 * @see ConfigurationListener
 * @see ConfigurationNotificationEvent
 */
public interface ConfigurationNotificationEvent<VIEWT> {
    /**
     * Returns the previous value of the updated configuration.
     *
     * @return Previous value of the updated configuration.
     */
    @Nullable VIEWT oldValue();

    /**
     * Returns updated value of the configuration.
     *
     * @return Updated value of the configuration.
     */
    @Nullable VIEWT newValue();

    /**
     * Returns monotonously increasing counter, linked to the specific storage for current configuration values. Gives a unique change
     * identifier inside a specific configuration storage.
     *
     * @return Counter value.
     */
    long storageRevision();

    /**
     * Returns the parent (any from the root) or current configuration.
     *
     * <p>For example, if we changed the child configuration, then we can get both the parent and the current child configuration.
     *
     * @param configClass Configuration interface, for example {@code RootConfiguration}.
     * @param <T>         Configuration type.
     * @return Configuration instance.
     */
    @Nullable <T extends ConfigurationProperty> T config(Class<? extends ConfigurationProperty> configClass);

    /**
     * Returns the key of a named list item for the parent (any from the root) or current configuration.
     *
     * <p>For example, if a column of a table has changed, then we can get the name of the table and columns for which the changes have
     * occurred.
     *
     * @param configClass Configuration interface, for example {@code TableConfiguration}.
     * @param <T>         Configuration type.
     * @return Configuration instance.
     */
    @Nullable <T extends ConfigurationProperty> String name(Class<? extends ConfigurationProperty> configClass);
}
