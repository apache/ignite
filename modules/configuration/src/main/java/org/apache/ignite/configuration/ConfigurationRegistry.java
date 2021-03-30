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
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.RootKeyImpl;
import org.apache.ignite.configuration.internal.SuperRoot;
import org.apache.ignite.configuration.internal.util.ConfigurationUtil;
import org.apache.ignite.configuration.internal.util.KeyNotFoundException;
import org.apache.ignite.configuration.internal.validation.MaxValidator;
import org.apache.ignite.configuration.internal.validation.MinValidator;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.Validator;

/** */
public class ConfigurationRegistry {
    /** */
    private final Map<String, DynamicConfiguration<?, ?, ?>> configs = new HashMap<>();

    /** */
    private final ConfigurationChanger changer = new ConfigurationChanger();

    {
        // Default vaildators implemented in current module.
        changer.addValidator(Min.class, new MinValidator());
        changer.addValidator(Max.class, new MaxValidator());
    }

    /** */
    public void registerRootKey(RootKey<?, ?> rootKey) {
        changer.addRootKey(rootKey);

        configs.put(rootKey.key(), (DynamicConfiguration<?, ?, ?>)rootKey.createPublicRoot(changer));

        //TODO IGNITE-14180 link these two entities.
    }

    /** */
    public <A extends Annotation> void registerValidator(Class<A> annotationType, Validator<A, ?> validator) {
        changer.addValidator(annotationType, validator);
    }

    /** */
    public void registerStorage(ConfigurationStorage configurationStorage) {
        changer.register(configurationStorage);
    }

    /** */
    public <V, C, T extends ConfigurationTree<V, C>> T getConfiguration(RootKey<T, V> rootKey) {
        return (T)configs.get(rootKey.key());
    }

    /**
     * Convert configuration subtree into a user-defined representation.
     *
     * @param path Path to configuration subtree. Can be empty, can't be {@code null}.
     * @param visitor Visitor that will be applied to the subtree and build the representation.
     * @param <T> Type of the representation.
     * @return User-defined representation constructed by {@code visitor}.
     * @throws IllegalArgumentException If {@code path} is not found in current configuration.
     */
    public <T> T represent(List<String> path, ConfigurationVisitor<T> visitor) throws IllegalArgumentException {
        SuperRoot mergedSuperRoot = changer.mergedSuperRoot();

        Object node;
        try {
            node = ConfigurationUtil.find(path, mergedSuperRoot);
        }
        catch (KeyNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        if (node instanceof TraversableTreeNode)
            return ((TraversableTreeNode)node).accept(null, visitor);

        assert node == null || node instanceof Serializable;

        return visitor.visitLeafNode(null, (Serializable)node);
    }

    /** */
    public CompletableFuture<?> change(List<String> path, ConfigurationSource changesSource, ConfigurationStorage storage) {
        return changer.changeX(path, changesSource, storage);
    }

    /**
     * Method to instantiate a new {@link RootKey} for your configuration root. Invoked in generated code only.
     * Does not register this root anywhere, used for static object initialization only.
     *
     * @param rootName Name of the root as described in {@link ConfigurationRoot#rootName()}.
     * @param storageType Storage class as described in {@link ConfigurationRoot#storage()}.
     * @param rootSupplier Closure to instantiate internal configuration tree roots.
     * @param publicRootCreator Function to create public user-facing tree instance.
     */
    public static <T extends ConfigurationTree<V, ?>, V> RootKey<T, V> newRootKey(
        String rootName,
        Class<? extends ConfigurationStorage> storageType,
        Supplier<InnerNode> rootSupplier,
        BiFunction<RootKey<T, V>, ConfigurationChanger, T> publicRootCreator
    ) {
        return new RootKeyImpl<>(rootName, storageType, rootSupplier, publicRootCreator);
    }
}
