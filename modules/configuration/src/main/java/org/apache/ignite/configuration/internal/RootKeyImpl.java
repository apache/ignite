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

import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.tree.InnerNode;

/** */
public class RootKeyImpl<T extends ConfigurationTree<VIEW, ?>, VIEW> extends RootKey<T, VIEW> {
    /** */
    private final String rootName;

    /** */
    private final ConfigurationType storageType;

    /** */
    private final Supplier<InnerNode> rootSupplier;

    /** */
    private final BiFunction<RootKey<T, VIEW>, ConfigurationChanger, T> publicRootCreator;

    /**
     * @param rootName Name of the root as described in {@link ConfigurationRoot#rootName()}.
     * @param storageType Storage class as described in {@link ConfigurationRoot#type()}.
     * @param rootSupplier Closure to instantiate internal configuration tree roots.
     * @param publicRootCreator Function to create a public user-facing tree instance.
     */
    public RootKeyImpl(
        String rootName,
        ConfigurationType storageType,
        Supplier<InnerNode> rootSupplier,
        BiFunction<RootKey<T, VIEW>, ConfigurationChanger, T> publicRootCreator
    ) {
        this.rootName = rootName;
        this.storageType = storageType;
        this.rootSupplier = rootSupplier;
        this.publicRootCreator = publicRootCreator;
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return rootName;
    }

    /** {@inheritDoc} */
    @Override protected ConfigurationType type() {
        return storageType;
    }

    /** {@inheritDoc} */
    @Override protected InnerNode createRootNode() {
        return rootSupplier.get();
    }

    /** {@inheritDoc} */
    @Override protected T createPublicRoot(ConfigurationChanger changer) {
        return publicRootCreator.apply(this, changer);
    }
}
