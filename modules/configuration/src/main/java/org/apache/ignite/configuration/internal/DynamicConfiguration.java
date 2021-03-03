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

import java.io.Serializable;
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
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.FieldValidator;

/**
 * This class represents configuration root or node.
 */
public abstract class DynamicConfiguration<VIEW, INIT, CHANGE> extends ConfigurationNode<VIEW> implements ConfigurationProperty<VIEW, CHANGE>, ConfigurationTree<VIEW, CHANGE> {

    /** Configuration members (leaves and nodes). */
    protected final Map<String, ConfigurationProperty<?, ?>> members = new HashMap<>();

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
        RootKey<?> rootKey,
        ConfigurationChanger changer
    ) {
        super(prefix, key, rootKey, changer);
    }

    /**
     * Add new configuration member.
     * @param member Configuration member (leaf or node).
     * @param <P> Type of member.
     */
    protected <P extends ConfigurationProperty<?, ?>> void add(P member) {
        members.put(member.key(), member);
    }

    /**
     * Add new configuration member with validators.
     * @param member Configuration member (leaf or node).
     * @param validators Validators for new member.
     * @param <PROP> Type of {@link DynamicProperty}.
     * @param <M> Type of member.
     */
    protected <PROP extends Serializable, M extends DynamicProperty<PROP>> void add(
        M member,
        List<FieldValidator<? super PROP, ? extends ConfigurationTree<?, ?>>> validators
    ) {
        members.put(member.key(), member);

        //TODO IGNITE-14183
//        configurator.addValidations((Class<? extends ConfigurationTree<?, ?>>) getClass(), member.key(), validators);
    }

    /** {@inheritDoc} */
    @Override public Future<Void> change(Consumer<CHANGE> change) throws ConfigurationValidationException {
        Objects.requireNonNull(change, "Configuration consumer cannot be null.");

        InnerNode rootNodeChange = ((RootKeyImpl)rootKey).createRootNode();

        if (keys.size() == 1) {
            // Current node is a root.
            change.accept((CHANGE)rootNodeChange);
        }
        else {
            assert keys instanceof RandomAccess;

            // Transform inner node closure into update tree.
            rootNodeChange.construct(keys.get(1), new ConfigurationSource() {
                private int level = 1;

                @Override public void descend(ConstructableTreeNode node) {
                    if (++level == keys.size())
                        change.accept((CHANGE)node);
                    else
                        node.construct(keys.get(level), this);
                }
            });
        }

        // Use resulting tree as update request for the storage.
        return changer.change(Map.of(rootKey, rootNodeChange));
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

    /** {@inheritDoc} */
    @Override protected void beforeRefreshValue(VIEW newValue) {
        // No-op.
    }
}
