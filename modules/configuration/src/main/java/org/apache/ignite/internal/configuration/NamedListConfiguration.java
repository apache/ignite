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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.internal.configuration.direct.DirectNamedListProxy;
import org.apache.ignite.internal.configuration.direct.DirectPropertyProxy;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;

/**
 * Named configuration wrapper.
 */
public class NamedListConfiguration<T extends ConfigurationProperty<VIEWT>, VIEWT, CHANGET extends VIEWT>
        extends DynamicConfiguration<NamedListView<VIEWT>, NamedListChange<VIEWT, CHANGET>>
        implements NamedConfigurationTree<T, VIEWT, CHANGET> {
    /** Listeners of property update. */
    private final List<ConfigurationNamedListListener<VIEWT>> extendedListeners = new CopyOnWriteArrayList<>();

    /** Creator of named configuration. */
    private final BiFunction<List<String>, String, T> cfgCreator;

    /** Creator of direct proxies for inner nodes. */
    private BiFunction<List<KeyPathNode>, DynamicConfigurationChanger, T> directProxyCreator;

    /** Placeholder to add listeners for any configuration. */
    private final T anyConfig;

    /** Notification cache. */
    private volatile Map<String, T> notificationCache = Collections.emptyMap();

    /**
     * Constructor.
     *
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     * @param listenOnly Only adding listeners mode, without the ability to get or update the property value.
     * @param cfgCreator Underlying configuration creator function.
     * @param directProxyCreator Creator for direct proxies of elements.
     *      Required for {@link DirectPropertyProxy#DirectPropertyProxy(List, DynamicConfigurationChanger)}.
     * @param anyConfig Placeholder to add listeners for any configuration.
     */
    public NamedListConfiguration(
            List<String> prefix,
            String key,
            RootKey<?, ?> rootKey,
            DynamicConfigurationChanger changer,
            boolean listenOnly,
            BiFunction<List<String>, String, T> cfgCreator,
            BiFunction<List<KeyPathNode>, DynamicConfigurationChanger, T> directProxyCreator,
            T anyConfig
    ) {
        super(prefix, key, rootKey, changer, listenOnly);

        this.cfgCreator = cfgCreator;
        this.directProxyCreator = directProxyCreator;
        this.anyConfig = anyConfig;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public T get(String name) {
        DynamicConfiguration<?, ?> config = getConfig(name);

        return config == null ? null : (T) config.specificConfigTree();
    }

    /**
     * Get a raw named config.
     *
     * @param name Name.
     * @return Raw config.
     */
    @Nullable
    public DynamicConfiguration<?, ?> getConfig(String name) {
        refreshValue();

        return (DynamicConfiguration<?, ?>) members.get(name);
    }

    /**
     * Retrieves a named list element by its internal id.
     *
     * @param internalId Internal id.
     * @return Named list element, associated with the passed internal id, or {@code null} if it doesn't exist.
     */
    public T getByInternalId(UUID internalId) {
        var value = (NamedListNode<?>) value();

        return (T) members.get(value.keyByInternalId(internalId));
    }

    /** {@inheritDoc} */
    @Override
    protected synchronized void beforeRefreshValue(
            NamedListView<VIEWT> newValue,
            @Nullable NamedListView<VIEWT> oldValue
    ) {
        Map<String, ConfigurationProperty<?>> oldValues = this.members;
        Map<String, ConfigurationProperty<?>> newValues = new LinkedHashMap<>();

        for (String key : newValue.namedListKeys()) {
            ConfigurationProperty<?> oldElement = oldValues.get(key);

            if (oldElement != null) {
                newValues.put(key, oldElement);
            } else {
                newValues.put(key, cfgCreator.apply(keys, key));
            }
        }

        this.members = newValues;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, ConfigurationProperty<?>> touchMembers() {
        Map<String, T> res = notificationCache;

        refreshValue();

        notificationCache = (Map<String, T>) members;

        return Collections.unmodifiableMap(res);
    }

    /**
     * Returns list of listeners that are specific for named configurations.
     *
     * @return List of listeners that are specific for named configurations.
     */
    public List<ConfigurationNamedListListener<VIEWT>> extendedListeners() {
        return Collections.unmodifiableList(extendedListeners);
    }

    /** {@inheritDoc} */
    @Override
    public void listenElements(ConfigurationNamedListListener<VIEWT> listener) {
        extendedListeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override
    public void stopListenElements(ConfigurationNamedListListener<VIEWT> listener) {
        extendedListeners.remove(listener);
    }

    /** {@inheritDoc} */
    @Override
    public T any() {
        return anyConfig;
    }

    /** {@inheritDoc} */
    @Override
    public Class<? extends ConfigurationProperty<NamedListView<VIEWT>>> configType() {
        throw new UnsupportedOperationException("Not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public NamedListView<VIEWT> value() {
        return refreshValue();
    }

    /** {@inheritDoc} */
    @Override
    public DirectPropertyProxy<NamedListView<VIEWT>> directProxy() {
        if (listenOnly) {
            throw listenOnlyException();
        }

        assert directProxyCreator != null;

        return new DirectNamedListProxy<>(keyPath(), changer, directProxyCreator);
    }
}
