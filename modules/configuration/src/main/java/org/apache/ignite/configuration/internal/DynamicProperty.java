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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.Future;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.PropertyListener;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;

/**
 * Holder for property value. Expected to be used with numbers, strings and other immutable objects, e.g. IP addresses.
 */
public class DynamicProperty<T extends Serializable> extends ConfigurationNode<T> implements ConfigurationProperty<T, T>, ConfigurationValue<T> {
    /** Listeners of property update. */
    private final List<PropertyListener<T, T>> updateListeners = new ArrayList<>();

    /**
     * Constructor.
     * @param prefix Property prefix.
     * @param key Property name.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     */
    public DynamicProperty(
        List<String> prefix,
        String key,
        RootKey<?> rootKey,
        ConfigurationChanger changer
    ) {
        super(prefix, key, rootKey, changer);
    }

    /**
     * Add change listener to this property.
     * @param listener Property change listener.
     */
    public void addListener(PropertyListener<T, T> listener) {
        updateListeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public T value() {
        return refreshValue();
    }

    /** {@inheritDoc} */
    @Override public Future<Void> update(T newValue) throws ConfigurationValidationException {
        Objects.requireNonNull(newValue, "Configuration value cannot be null.");

        InnerNode rootNodeChange = ((RootKeyImpl)rootKey).createRootNode();

        assert keys instanceof RandomAccess;
        assert !keys.isEmpty();

        rootNodeChange.construct(keys.get(1), new ConfigurationSource() {
            private int i = 1;

            @Override public void descend(ConstructableTreeNode node) {
                assert i < keys.size() - 1;

                node.construct(keys.get(++i), this);
            }

            @Override public <T> T unwrap(Class<T> clazz) {
                assert i == keys.size() - 1;

                assert clazz.isInstance(newValue);

                return clazz.cast(newValue);
            }
        });

        return changer.change(Map.of(rootKey, rootNodeChange));
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override protected void refreshValue0(T newValue) {
        // No-op.
    }
}
