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

package org.apache.ignite.internal.configuration.direct;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.appendKey;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;

/**
 * {@link DirectPropertyProxy} implementation for named lists.
 */
public class DirectNamedListProxy<T extends ConfigurationProperty<VIEWT>, VIEWT, CHANGET extends VIEWT>
        extends DirectPropertyProxy<NamedListView<VIEWT>>
        implements NamedConfigurationTree<T, VIEWT, CHANGET> {
    /**
     * Factory to instantiate underlying proxied named list elements.
     */
    private final BiFunction<List<KeyPathNode>, DynamicConfigurationChanger, T> creator;

    /**
     * Constructor.
     *
     * @param keys Full path to the node.
     * @param changer Changer.
     * @param creator Factory to instantiate underlying proxied named list elements.
     */
    public DirectNamedListProxy(
            List<KeyPathNode> keys,
            DynamicConfigurationChanger changer,
            BiFunction<List<KeyPathNode>, DynamicConfigurationChanger, T> creator
    ) {
        super(keys, changer);

        this.creator = creator;
    }

    /** {@inheritDoc} */
    @Override public T get(String name) {
        return creator.apply(appendKey(keys, new KeyPathNode(name, true)), changer);
    }

    /**
     * Retrieves a named list element by its internal id.
     *
     * @param internalId Internal id.
     * @return Named list element, associated with the passed internal id, or {@code null} if it doesn't exist.
     */
    public T getByInternalId(UUID internalId) {
        return creator.apply(appendKey(keys, new KeyPathNode(internalId.toString(), false)), changer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> change(Consumer<NamedListChange<VIEWT, CHANGET>> change) {
        throw new UnsupportedOperationException("change");
    }

    /** {@inheritDoc} */
    @Override public void listenElements(ConfigurationNamedListListener<VIEWT> listener) {
        throw new UnsupportedOperationException("listenElements");
    }

    /** {@inheritDoc} */
    @Override public void stopListenElements(ConfigurationNamedListListener<VIEWT> listener) {
        throw new UnsupportedOperationException("stopListenElements");
    }

    /** {@inheritDoc} */
    @Override public T any() {
        throw new UnsupportedOperationException("any");
    }
}
