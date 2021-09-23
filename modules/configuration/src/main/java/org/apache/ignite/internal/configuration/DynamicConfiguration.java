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

package org.apache.ignite.internal.configuration;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationNotificationsUtil;

/**
 * This class represents configuration root or node.
 */
public abstract class DynamicConfiguration<VIEW, CHANGE> extends ConfigurationNode<VIEW>
    implements ConfigurationTree<VIEW, CHANGE>
{
    /** Configuration members (leaves and nodes). */
    protected volatile Map<String, ConfigurationProperty<?>> members = new LinkedHashMap<>();

    /**
     * Constructor.
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     */
    public DynamicConfiguration(
        List<String> prefix,
        String key,
        RootKey<?, ?> rootKey,
        DynamicConfigurationChanger changer
    ) {
        super(prefix, key, rootKey, changer);
    }

    /**
     * Add new configuration member.
     * @param member Configuration member (leaf or node).
     * @param <P> Type of member.
     */
    protected final <P extends ConfigurationProperty<?>> void add(P member) {
        members.put(member.key(), member);
    }

    /** {@inheritDoc} */
    @Override public final CompletableFuture<Void> change(Consumer<CHANGE> change) {
        Objects.requireNonNull(change, "Configuration consumer cannot be null.");

        assert keys instanceof RandomAccess;

        ConfigurationSource src = new ConfigurationSource() {
            /** Current index in the {@code keys}. */
            private int level = 0;

            /** {@inheritDoc} */
            @Override public void descend(ConstructableTreeNode node) {
                if (level == keys.size())
                    change.accept((CHANGE)node);
                else
                    node.construct(keys.get(level++), this, true);
            }

            /** {@inheritDoc} */
            @Override public void reset() {
                level = 0;
            }
        };

        // Use resulting tree as update request for the storage.
        return changer.change(src);
    }

    /** {@inheritDoc} */
    @Override public final String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public final VIEW value() {
        return refreshValue();
    }

    /**
     * Returns all child nodes of the current configuration tree node.
     *
     * @return Map from childs keys to a corresponding {@link ConfigurationProperty}.
     */
    public Map<String, ConfigurationProperty<?>> members() {
        refreshValue();

        return Collections.unmodifiableMap(members);
    }

    /**
     * Touches current Dynamic Configuration node. Currently this method makes sense for {@link NamedListConfiguration}
     * class only, but this will be changed in <a href="https://issues.apache.org/jira/browse/IGNITE-14645">IGNITE-14645
     * </a>.
     * Method is invoked on configuration initialization and on every configuration update, even those that don't affect
     * current node. Its goal is to have a fine control over sub-nodes of the configuration. Accessor methods on the
     * Dynamic Configuration nodes can be called at any time and have to return up-to-date value. This means that one
     * can read updated configuration value before notification listeners have been invoked on it. At that point, for
     * example, deleted named list elements disappear from the object and cannot be accessed with a regular API. The
     * only way to access them is to have a cached copy of all elements (members). This method does exactly that. It
     * returns cached copy of members and then sets it to a new, maybe different, set of members. No one except for
     * {@link ConfigurationNotificationsUtil} should ever call this method.
     *
     * @return Members map associated with "previous" node state.
     */
    public Map<String, ConfigurationProperty<?>> touchMembers() {
        return members();
    }
}
