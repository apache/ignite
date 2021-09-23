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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;

/**
 * Super class for dynamic configuration tree nodes. Has all common data and value retrieving algorithm in it.
 */
public abstract class ConfigurationNode<VIEW> implements ConfigurationProperty<VIEW> {
    /** Listeners of property update. */
    private final List<ConfigurationListener<VIEW>> updateListeners = new CopyOnWriteArrayList<>();

    /** Full path to the current node. */
    protected final List<String> keys;

    /** Name of the current node. Same as last element of {@link #keys}. */
    protected final String key;

    /** Root key instance for the current trees root. */
    protected final RootKey<?, ?> rootKey;

    /** Configuration changer instance to get latest value of the root. */
    protected final DynamicConfigurationChanger changer;

    /**
     * Cached value of current trees root. Useful to determine whether you have the latest configuration value or not.
     */
    private volatile TraversableTreeNode cachedRootNode;

    /** Cached configuration value. Immutable. */
    private VIEW val;

    /**
     * Validity flag. Configuration is declared invalid if it's a part of named list configuration and corresponding
     * entry is already removed.
     */
    private boolean invalid;

    /**
     * Constructor.
     *
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     */
    protected ConfigurationNode(List<String> prefix, String key, RootKey<?, ?> rootKey, DynamicConfigurationChanger changer) {
        this.keys = ConfigurationUtil.appendKey(prefix, key);
        this.key = key;
        this.rootKey = rootKey;
        this.changer = changer;

        assert Objects.equals(rootKey.key(), keys.get(0));
    }

    /** {@inheritDoc} */
    @Override public void listen(ConfigurationListener<VIEW> listener) {
        updateListeners.add(listener);
    }

    /** @return List of update listeners. */
    public List<ConfigurationListener<VIEW>> listeners() {
        return Collections.unmodifiableList(updateListeners);
    }

    /**
     * Returns latest value of the configuration or throws exception.
     *
     * @return Latest configuration value.
     * @throws NoSuchElementException If configuration is a part of already deleted named list configuration entry.
     */
    protected final VIEW refreshValue() throws NoSuchElementException {
        TraversableTreeNode newRootNode = changer.getRootNode(rootKey);
        TraversableTreeNode oldRootNode = cachedRootNode;

        // 'invalid' and 'val' visibility is guaranteed by the 'cachedRootNode' volatile read
        if (invalid)
            throw noSuchElementException();

        if (oldRootNode == newRootNode)
            return val;

        try {
            VIEW newVal = ConfigurationUtil.find(keys.subList(1, keys.size()), newRootNode, true);

            synchronized (this) {
                if (cachedRootNode == oldRootNode) {
                    beforeRefreshValue(newVal);

                    val = newVal;

                    cachedRootNode = newRootNode;

                    return newVal;
                }
                else {
                    if (invalid)
                        throw noSuchElementException();

                    return val;
                }
            }
        }
        catch (KeyNotFoundException e) {
            synchronized (this) {
                invalid = true;

                cachedRootNode = newRootNode;
            }

            throw noSuchElementException();
        }
    }

    /**
     * @return Exception instance with a proper error message.
     */
    private NoSuchElementException noSuchElementException() {
        return new NoSuchElementException(ConfigurationUtil.join(keys));
    }

    /**
     * Callback from {@link #refreshValue()} that's called right before the update. Synchronized.
     *
     * @param newValue New configuration value.
     */
    protected void beforeRefreshValue(VIEW newValue) {
        // No-op.
    }
}
