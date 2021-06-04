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

package org.apache.ignite.configuration.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;

/**
 * This class represents configuration root or node.
 */
public abstract class DynamicConfiguration<VIEW, CHANGE> extends ConfigurationNode<VIEW, CHANGE>
    implements ConfigurationTree<VIEW, CHANGE>
{
    /** Configuration members (leaves and nodes). */
    private final Map<String, ConfigurationProperty<?, ?>> members = new HashMap<>();

    /**
     * Constructor.
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     */
    protected DynamicConfiguration(
        List<String> prefix,
        String key,
        RootKey<?, ?> rootKey,
        ConfigurationChanger changer
    ) {
        super(prefix, key, rootKey, changer);
    }

    /**
     * Add new configuration member.
     * @param member Configuration member (leaf or node).
     * @param <P> Type of member.
     */
    protected final <P extends ConfigurationProperty<?, ?>> void add(P member) {
        members.put(member.key(), member);
    }

    /** {@inheritDoc} */
    @Override public final Future<Void> change(Consumer<CHANGE> change) {
        Objects.requireNonNull(change, "Configuration consumer cannot be null.");

        assert keys instanceof RandomAccess;

        ConfigurationSource src = new ConfigurationSource() {
            private int level = 0;

            @Override public void descend(ConstructableTreeNode node) {
                if (level == keys.size())
                    change.accept((CHANGE)node);
                else
                    node.construct(keys.get(level++), this);
            }

            @Override public void reset() {
                level = 0;
            }
        };

        // Use resulting tree as update request for the storage.
        return changer.change(src, null);
    }

    /** {@inheritDoc} */
    @Override public final String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public final VIEW value() {
        return refreshValue();
    }

    /** {@inheritDoc} */
    @Override public Map<String, ConfigurationProperty<?, ?>> members() {
        return Collections.unmodifiableMap(members);
    }
}
