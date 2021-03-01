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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.DynamicProperty;
import org.apache.ignite.configuration.internal.validation.MemberKey;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.FieldValidator;
import org.apache.ignite.configuration.validation.ValidationIssue;

/**
 * Convenient wrapper for configuration root. Provides access to configuration tree, stores validators, performs actions
 * on configuration such as initialized, change and view.
 * @param <T> Type of configuration root.
 */
public class Configurator<T extends DynamicConfiguration<?, ?, ?>> {
    /** Root of the configuration tree. */
    private final T root;

    /** Configuration property validators. */
    private final Map<MemberKey, List<FieldValidator<? extends Serializable, T>>> fieldValidators = new HashMap<>();

    /**
     *
     * @param rootBuilder
     * @param <VIEW>
     * @param <INIT>
     * @param <CHANGE>
     * @param <CONF>
     * @return
     */
    public static <VIEW, INIT, CHANGE, CONF extends DynamicConfiguration<VIEW, INIT, CHANGE>> Configurator<CONF> create(
        Function<Configurator<CONF>, CONF> rootBuilder
    ) {
        return new Configurator<>(rootBuilder);
    }

    /**
     * Constructor.
     * @param rootBuilder Function, that creates configuration root.
     */
    private <VIEW, INIT, CHANGE, CONF extends DynamicConfiguration<VIEW, INIT, CHANGE>> Configurator(
        Function<Configurator<CONF>, CONF> rootBuilder
    ) {
        final CONF built = rootBuilder.apply((Configurator<CONF>) this);

        root = (T) built;
    }

    /**
     *
     * @param aClass
     * @param key
     * @param validators
     * @param <PROP>
     */
    public <PROP extends Serializable> void addValidations(
        Class<? extends ConfigurationTree<?, ?>> aClass,
        String key,
        List<FieldValidator<? super PROP, ? extends ConfigurationTree<?, ?>>> validators
    ) {
        fieldValidators.put(new MemberKey(aClass, key), (List) validators);
    }

    /**
     * Get all validators for given member key (class + field).
     * @param key Member key.
     * @return Validators.
     */
    public List<FieldValidator<? extends Serializable, T>> validators(MemberKey key) {
        return fieldValidators.getOrDefault(key, Collections.emptyList());
    }

    /**
     * Get configuration root.
     * @return Configuration root.
     */
    public T getRoot() {
        return root;
    }

    /**
     * Execute on property attached to configurator.
     * @param property Property.
     * @param <PROP> Type of the property.
     */
    public <PROP extends Serializable> void onAttached(DynamicProperty<PROP> property) {
        property.addListener(new PropertyListener<PROP, PROP>() {
            /** {@inheritDoc} */
            @Override public void update(PROP newValue, ConfigurationProperty<PROP, PROP> modifier) {
//                storage.save(key, newValue);
            }
        });
//        storage.listen(key, property::setSilently);
    }

    public List<ValidationIssue> validateChanges(TraversableTreeNode changes) {
        return Collections.emptyList();
    }
}
