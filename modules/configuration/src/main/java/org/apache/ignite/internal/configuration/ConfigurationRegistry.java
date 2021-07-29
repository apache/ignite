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

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.Max;
import org.apache.ignite.configuration.validation.Min;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;
import org.apache.ignite.internal.configuration.validation.ImmutableValidator;
import org.apache.ignite.internal.configuration.validation.MaxValidator;
import org.apache.ignite.internal.configuration.validation.MinValidator;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.configuration.util.ConfigurationNotificationsUtil.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;

/** */
public class ConfigurationRegistry implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ConfigurationRegistry.class);

    /** */
    private final Map<String, DynamicConfiguration<?, ?>> configs = new HashMap<>();

    /** Root keys. */
    private final Collection<RootKey<?, ?>> rootKeys;

    /** Validators. */
    private final Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators;

    /** Configuration storages. */
    private final Collection<ConfigurationStorage> configurationStorages;

    /** */
    private volatile ConfigurationChanger changer;

    /** */
    private final ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

    /**
     * Constructor.
     *
     * @param rootKeys Configuration root keys.
     * @param validators Validators.
     * @param configurationStorages Configuration Storages.
     */
    public ConfigurationRegistry(
        Collection<RootKey<?, ?>> rootKeys,
        Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators,
        Collection<ConfigurationStorage> configurationStorages
    ) {
        this.rootKeys = rootKeys;
        this.validators = validators;
        this.configurationStorages = configurationStorages;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        this.changer = new ConfigurationChanger(this::notificator) {
            /** {@inheritDoc} */
            @Override public InnerNode createRootNode(RootKey<?, ?> rootKey) {
                return cgen.instantiateNode(rootKey.schemaClass());
            }
        };

        changer.addValidator(Min.class, new MinValidator());
        changer.addValidator(Max.class, new MaxValidator());
        changer.addValidator(Immutable.class, new ImmutableValidator());

        rootKeys.forEach(rootKey -> {
            cgen.compileRootSchema(rootKey.schemaClass());

            changer.addRootKey(rootKey);

            DynamicConfiguration<?, ?> cfg = cgen.instantiateCfg(rootKey, changer);

            configs.put(rootKey.key(), cfg);
        });

        validators.forEach(changer::addValidators);

        configurationStorages.forEach(changer::register);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (changer != null)
            changer.stop();
    }

    /**
     * Starts storage configurations.
     * @param storageType Storage type.
     */
    public void startStorageConfigurations(ConfigurationType storageType) {
        changer.initialize(storageType);
    }

    /**
     * Gets the public configuration tree.
     * @param rootKey Root key.
     * @param <V> View type.
     * @param <C> Change type.
     * @param <T> Configuration tree type.
     * @return Public configuration tree.
     */
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

    /**
     * Change configuration.
     * @param changesSource Configuration source to create patch from it.
     * @param storage Expected storage for the changes. Can be null, this will mean that derived storage will be used
     * unconditionaly.
     * @return Future that is completed on change completion.
     */
    public CompletableFuture<Void> change(ConfigurationSource changesSource, @Nullable ConfigurationStorage storage) {
        return changer.change(changesSource, storage);
    }

    /** */
    private @NotNull CompletableFuture<Void> notificator(SuperRoot oldSuperRoot, SuperRoot newSuperRoot, long storageRevision) {
        List<CompletableFuture<?>> futures = new ArrayList<>();

        newSuperRoot.traverseChildren(new ConfigurationVisitor<Void>() {
            @Override public Void visitInnerNode(String key, InnerNode newRoot) {
                InnerNode oldRoot = oldSuperRoot.traverseChild(key, innerNodeVisitor());

                var cfg = (DynamicConfiguration<InnerNode, ?>)configs.get(key);

                assert oldRoot != null && cfg != null : key;

                if (oldRoot != newRoot)
                    notifyListeners(oldRoot, newRoot, cfg, storageRevision, futures);

                return null;
            }
        });

        // Map futures into a "suppressed" future that won't throw any exceptions on completion.
        Function<CompletableFuture<?>, CompletableFuture<?>> mapping = fut -> fut.handle((res, throwable) -> {
            if (throwable != null)
                LOG.error("Failed to notify configuration listener.", throwable);

            return res;
        });

        CompletableFuture[] resultFutures = futures.stream().map(mapping).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(resultFutures);
    }
}
