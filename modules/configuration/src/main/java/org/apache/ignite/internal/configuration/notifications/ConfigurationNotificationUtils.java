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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Useful class for notifying configuration listeners.
 */
class ConfigurationNotificationUtils {
    /**
     * Private constructor.
     */
    private ConfigurationNotificationUtils() {
    }

    /**
     * Returns the dynamic property of the leaf.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName      Name of the child node.
     * @return Dynamic property of a leaf or {@code null} if the leaf does not exist.
     */
    static @Nullable DynamicProperty<Serializable> dynamicProperty(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (DynamicProperty<Serializable>) dynamicConfig.members().get(nodeName);
    }

    /**
     * Returns the dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName      Name of the child node.
     * @return Dynamic configuration of the child node or {@code null} if the child node does not exist.
     */
    static @Nullable DynamicConfiguration<InnerNode, ?> dynamicConfig(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (DynamicConfiguration<InnerNode, ?>) dynamicConfig.members().get(nodeName);
    }

    /**
     * Returns the named dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName      Name of the child node.
     * @return Named dynamic configuration of the child node or {@code null} if the child node does not exist.
     */
    static @Nullable NamedListConfiguration<?, InnerNode, ?> namedDynamicConfig(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (NamedListConfiguration<?, InnerNode, ?>) dynamicConfig.members().get(nodeName);
    }

    /**
     * Null-safe version of {@link ConfigurationNode#listeners()}.
     *
     * <p>Needed for working with "any" configuration properties, see this class' javadoc for details.
     *
     * @return Listeners of the given node or an empty list if it is {@code null}.
     */
    static <T> Collection<ConfigurationListener<T>> listeners(@Nullable ConfigurationNode<T> node) {
        return node == null ? List.of() : node.listeners();
    }

    /**
     * Null-safe version of {@link NamedListConfiguration#extendedListeners()}.
     *
     * <p>Needed for working with "any" configuration properties, see this class' javadoc for details.
     *
     * @return Listeners of the given node or an empty list if it is {@code null}.
     */
    static <T> Collection<ConfigurationNamedListListener<T>> extendedListeners(@Nullable NamedListConfiguration<?, T, ?> node) {
        return node == null ? List.of() : node.extendedListeners();
    }

    /**
     * Returns the dynamic configuration of the {@link NamedListConfiguration#any any} node.
     *
     * @param namedConfig Dynamic configuration.
     * @return Dynamic configuration of the "any" node.
     */
    static @Nullable DynamicConfiguration<InnerNode, ?> any(@Nullable NamedListConfiguration<?, InnerNode, ?> namedConfig) {
        return namedConfig == null ? null : (DynamicConfiguration<InnerNode, ?>) namedConfig.any();
    }

    /**
     * Merge {@link NamedListConfiguration#any "any"} configurations.
     *
     * @param anyConfigs Current {@link NamedListConfiguration#any "any"} configurations.
     * @param anyConfig  New {@link NamedListConfiguration#any "any"} configuration.
     * @return Merged {@link NamedListConfiguration#any "any"} configurations.
     */
    static Collection<DynamicConfiguration<InnerNode, ?>> mergeAnyConfigs(
            Collection<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            @Nullable DynamicConfiguration<InnerNode, ?> anyConfig
    ) {
        return Stream.concat(anyConfigs.stream(), Stream.of(anyConfig))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
