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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.tree.NamedListChange;
import org.apache.ignite.configuration.tree.NamedListInit;
import org.apache.ignite.configuration.tree.NamedListView;

/**
 * Named configuration wrapper.
 */
public class NamedListConfiguration<T extends ConfigurationProperty<VIEW, CHANGE>, VIEW, CHANGE, INIT>
    extends DynamicConfiguration<NamedListView<VIEW>, NamedListInit<INIT>, NamedListChange<CHANGE, INIT>>
    implements NamedConfigurationTree<T, VIEW, CHANGE, INIT>
{
    /** Creator of named configuration. */
    private final BiFunction<List<String>, String, T> creator;

    /** Named configurations. */
    private volatile Map<String, T> values = new HashMap<>();

    /**
     * Constructor.
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     * @param creator Underlying configuration creator function.
     */
    public NamedListConfiguration(
        List<String> prefix,
        String key,
        RootKey<?, ?> rootKey,
        ConfigurationChanger changer,
        BiFunction<List<String>, String, T> creator
    ) {
        super(prefix, key, rootKey, changer);
        this.creator = creator;
    }

    /** {@inheritDoc} */
    @Override public T get(String name) {
        refreshValue();

        return values.get(name);
    }

    /** {@inheritDoc} */
    @Override protected synchronized void beforeRefreshValue(NamedListView<VIEW> newValue) {
        Map<String, T> oldValues = this.values;
        Map<String, T> newValues = new HashMap<>();

        for (String key : newValue.namedListKeys()) {
            T oldElement = oldValues.get(key);

            if (oldElement != null)
                newValues.put(key, oldElement);
            else
                newValues.put(key, creator.apply(keys, key));
        }

        this.values = newValues;
    }
}
