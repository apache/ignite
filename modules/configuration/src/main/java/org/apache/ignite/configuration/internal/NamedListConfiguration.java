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
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.Configurator;

/**
 * Named configuration wrapper.
 */
public class NamedListConfiguration<VIEW, T extends Modifier<VIEW, INIT, CHANGE>, INIT, CHANGE>
    extends DynamicConfiguration<NamedList<VIEW>, NamedList<INIT>, NamedList<CHANGE>> {
    /** Creator of named configuration. */
    private final BiFunction<String, String, T> creator;

    /** Named configurations. */
    private final Map<String, T> values = new HashMap<>();

    /**
     * Constructor.
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param configurator Configurator that this object is attached to.
     * @param root Root configuration.
     * @param creator Underlying configuration creator function.
     */
    public NamedListConfiguration(
        String prefix,
        String key,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root,
        BiFunction<String, String, T> creator
    ) {
        super(prefix, key, false, configurator, root);
        this.creator = creator;
    }

    /**
     * Copy constructor.
     * @param base Base to copy from.
     * @param configurator Configurator to attach to.
     * @param root Root of the configuration.
     */
    private NamedListConfiguration(
        NamedListConfiguration<VIEW, T, INIT, CHANGE> base,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        super(base.prefix, base.key, false, configurator, root);

        this.creator = base.creator;

        for (Map.Entry<String, T> entry : base.values.entrySet()) {
            String k = entry.getKey();
            T value = entry.getValue();

            final T copy = (T) ((DynamicConfiguration<VIEW, INIT, CHANGE>) value).copy(root);
            add(copy);

            this.values.put(k, copy);
        }
    }

    /** {@inheritDoc} */
    @Override public void init(NamedList<INIT> list) {
        list.getValues().forEach((key, init) -> {
            if (!values.containsKey(key)) {
                final T created = creator.apply(qualifiedName, key);
                add(created);
                values.put(key, created);
            }

            values.get(key).init(init);
        });
    }

    /**
     * Get named configuration by name.
     * @param name Name.
     * @return Configuration.
     */
    public T get(String name) {
        return values.get(name);
    }

    /** {@inheritDoc} */
    @Override public NamedList<VIEW> value() {
        return new NamedList<>(values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().value())));
    }

    /** {@inheritDoc} */
    @Override public void changeWithoutValidation(NamedList<CHANGE> list) {
        list.getValues().forEach((key, change) -> {
            if (!values.containsKey(key)) {
                final T created = creator.apply(qualifiedName, key);
                add(created);
                values.put(key, created);
            }

            values.get(key).changeWithoutValidation(change);
        });
    }

    /** {@inheritDoc} */
    @Override public NamedListConfiguration<VIEW, T, INIT, CHANGE> copy(DynamicConfiguration<?, ?, ?> root) {
        return new NamedListConfiguration<>(this, configurator, root);
    }
}
