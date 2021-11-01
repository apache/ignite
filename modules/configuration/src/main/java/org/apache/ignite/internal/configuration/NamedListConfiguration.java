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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.jetbrains.annotations.Nullable;

/**
 * Named configuration wrapper.
 */
public class NamedListConfiguration<T extends ConfigurationProperty<VIEW>, VIEW, CHANGE extends VIEW>
    extends DynamicConfiguration<NamedListView<VIEW>, NamedListChange<VIEW, CHANGE>>
    implements NamedConfigurationTree<T, VIEW, CHANGE>
{
    /** Listeners of property update. */
    private final List<ConfigurationNamedListListener<VIEW>> extendedListeners = new CopyOnWriteArrayList<>();

    /** Creator of named configuration. */
    private final BiFunction<List<String>, String, T> creator;

    /** Placeholder to add listeners for any configuration. */
    private final T anyConfig;

    /** */
    private volatile Map<String, T> notificationCache = Collections.emptyMap();

    /**
     * Constructor.
     *
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     * @param listenOnly Only adding listeners mode, without the ability to get or update the property value.
     * @param creator Underlying configuration creator function.
     * @param anyConfig Placeholder to add listeners for any configuration.
     */
    public NamedListConfiguration(
        List<String> prefix,
        String key,
        RootKey<?, ?> rootKey,
        DynamicConfigurationChanger changer,
        boolean listenOnly,
        BiFunction<List<String>, String, T> creator,
        T anyConfig
    ) {
        super(prefix, key, rootKey, changer, listenOnly);

        this.creator = creator;
        this.anyConfig = anyConfig;
    }

    /** {@inheritDoc} */
    @Override public T get(String name) {
        refreshValue();

        ConfigurationProperty<?> configProperty = members.get(name);

        return configProperty == null ? null : (T)((DynamicConfiguration<?, ?>)configProperty).specificConfigTree();
    }

    /** {@inheritDoc} */
    @Override protected synchronized void beforeRefreshValue(
        NamedListView<VIEW> newValue,
        @Nullable NamedListView<VIEW> oldValue
    ) {
        Map<String, ConfigurationProperty<?>> oldValues = this.members;
        Map<String, ConfigurationProperty<?>> newValues = new LinkedHashMap<>();

        for (String key : newValue.namedListKeys()) {
            ConfigurationProperty<?> oldElement = oldValues.get(key);

            if (oldElement != null)
                newValues.put(key, oldElement);
            else
                newValues.put(key, creator.apply(keys, key));
        }

        this.members = newValues;
    }

    /** {@inheritDoc} */
    @Override public Map<String, ConfigurationProperty<?>> touchMembers() {
        Map<String, T> res = notificationCache;

        refreshValue();

        notificationCache = (Map<String, T>)members;

        return Collections.unmodifiableMap(res);
    }

    /** @return List of listeners that are specific for named configurations.*/
    public List<ConfigurationNamedListListener<VIEW>> extendedListeners() {
        return Collections.unmodifiableList(extendedListeners);
    }

    /** {@inheritDoc} */
    @Override public void listenElements(ConfigurationNamedListListener<VIEW> listener) {
        extendedListeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void stopListenElements(ConfigurationNamedListListener<VIEW> listener) {
        extendedListeners.remove(listener);
    }

    /** {@inheritDoc} */
    @Override public T any() {
        return anyConfig;
    }

    /** {@inheritDoc} */
    @Override public Class<? extends ConfigurationProperty<NamedListView<VIEW>>> configType() {
        throw new UnsupportedOperationException("Not supported.");
    }

    /** {@inheritDoc} */
    @Override public NamedListView<VIEW> value() {
        return refreshValue();
    }
}
