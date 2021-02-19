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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.tree.InnerNode;

/** */
public class ConfigurationRegistry {
    /** */
    private final Map<String, Configurator<?>> configs = new HashMap<>();

    /** */
    public <T extends DynamicConfiguration<?, ?, ?>> void registerConfigurator(Configurator<T> unitConfig) {
        String key = unitConfig.getRoot().key();

        configs.put(key, unitConfig);
    }

    /** */
    public <V, C, T extends ConfigurationTree<V, C>> T getConfiguration(RootKey<T> rootKey) {
        return (T) configs.get(rootKey.key()).getRoot();
    }

    /** */
    public Map<String, Configurator<? extends DynamicConfiguration<?, ?, ?>>> getConfigurators() {
        return Collections.unmodifiableMap(configs);
    }

    /**
     * Method to instantiate a new {@link RootKey} for your configuration root. Invoked in generated code only.
     * Does not register this root anywhere, used for static object initialization only.
     *
     * @param rootName Name of the root as described in {@link ConfigurationRoot#rootName()}.
     * @param storageType Storage class as descried in {@link ConfigurationRoot#storage()}.
     * @param rootSupplier Closure to instantiate internal configuration tree roots.
     */
    public static <T extends ConfigurationTree<?, ?>> RootKey<T> newRootKey(
        String rootName,
        Class<? extends ConfigurationStorage> storageType,
        Supplier<InnerNode> rootSupplier
    ) {
        return new RootKeyImpl<>(rootName, storageType, rootSupplier);
    }
}
