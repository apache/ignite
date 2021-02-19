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

import java.util.function.Supplier;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.tree.InnerNode;

/** */
class RootKeyImpl<T extends ConfigurationTree<?, ?>> extends RootKey<T> {
    /** */
    private final String rootName;

    /** */
    private final Class<? extends ConfigurationStorage> storageType;

    /** */
    private final Supplier<InnerNode> rootSupplier;

    /** */
    RootKeyImpl(String rootName, Class<? extends ConfigurationStorage> storageType, Supplier<InnerNode> rootSupplier) {
        this.rootName = rootName;
        this.storageType = storageType;
        this.rootSupplier = rootSupplier;
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return rootName;
    }

    /** {@inheritDoc} */
    @Override Class<? extends ConfigurationStorage> getStorageType() {
        return storageType;
    }

    /** {@inheritDoc} */
    @Override InnerNode createRootNode() {
        return rootSupplier.get();
    }
}
