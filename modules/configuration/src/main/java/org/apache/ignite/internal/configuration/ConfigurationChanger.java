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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.MemberKey;
import org.apache.ignite.internal.configuration.validation.ValidationUtil;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.dropNulls;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.toPrefixMap;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public abstract class ConfigurationChanger {
    /** */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** */
    private final Map<String, RootKey<?, ?>> rootKeys = new TreeMap<>();

    /** Map that has all the trees in accordance to their storages. */
    private final Map<ConfigurationType, StorageRoots> storagesRootsMap = new ConcurrentHashMap<>();

    /** Annotation classes mapped to validator objects. */
    private Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = new HashMap<>();

    /**
     * Closure interface to be used by the configuration changer. An instance of this closure is passed into the constructor and
     * invoked every time when there's an update from any of the storages.
     */
    @FunctionalInterface
    public interface Notificator {
        /**
         * Invoked every time when the configuration is updated.
         * @param oldRoot Old roots values. All these roots always belong to a single storage.
         * @param newRoot New values for the same roots as in {@code oldRoot}.
         * @param storageRevision Revision of the storage.
         * @return Not-null future that must signify when processing is completed. Exceptional completion is not
         *      expected.
         */
        @NotNull CompletableFuture<Void> notify(SuperRoot oldRoot, SuperRoot newRoot, long storageRevision);
    }

    /** Closure to execute when an update from the storage is received. */
    private final Notificator notificator;

    /**
     * Immutable data container to store version and all roots associated with the specific storage.
     */
    private static class StorageRoots {
        /** Immutable forest, so to say. */
        private final SuperRoot roots;

        /** Version associated with the currently known storage state. */
        private final long version;

        /** Future that signifies update of current configuration. */
        private final CompletableFuture<Void> changeFuture = new CompletableFuture<>();

        /** */
        private StorageRoots(SuperRoot roots, long version) {
            this.roots = roots;
            this.version = version;
        }
    }

    /** Lazy annotations cache for configuration schema fields. */
    private final Map<MemberKey, Annotation[]> cachedAnnotations = new ConcurrentHashMap<>();

    /** Storage instances by their classes. Comes in handy when all you have is {@link RootKey}. */
    private final Map<ConfigurationType, ConfigurationStorage> storageInstances = new HashMap<>();

    /**
     * @param notificator Closure to execute when update from the storage is received.
     */
    public ConfigurationChanger(Notificator notificator) {
        this.notificator = notificator;
    }

    /**
     * Adds a single validator instance.
     * @param annotationType Annotation type for validated fields.
     * @param validator Validator instance for this annotation.
     * @param <A> Annotation type.
     */
    public <A extends Annotation> void addValidator(Class<A> annotationType, Validator<A, ?> validator) {
        validators
            .computeIfAbsent(annotationType, a -> new HashSet<>())
            .add(validator);
    }

    /**
     * Adds multiple validators instances.
     * @param annotationType Annotation type for validated fields.
     * @param validators Set of validator instancec for this annotation.
     */
    public void addValidators(Class<? extends Annotation> annotationType, Set<Validator<? extends Annotation, ?>> validators) {
        this.validators
            .computeIfAbsent(annotationType, a -> new HashSet<>())
            .addAll(validators);
    }

    /**
     * Registers an additional root key.
     * @param rootKey Root key instance.
     */
    public void addRootKey(RootKey<?, ?> rootKey) {
        assert !storageInstances.containsKey(rootKey.type());

        rootKeys.put(rootKey.key(), rootKey);
    }

    /**
     * Created new {@code Node} object that corresponds to passed root keys root configuration node.
     * @param rootKey Root key.
     * @return New {@link InnerNode} instance that represents root.
     */
    public abstract InnerNode createRootNode(RootKey<?, ?> rootKey);

    /**
     * Utility method to create {@link SuperRoot} parameter value.
     * @return Function that creates root node by root name or returns {@code null} if root name is not found.
     */
    @NotNull private Function<String, InnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : createRootNode(rootKey);
        };
    }

    /**
     * Registers a storage.
     * @param configurationStorage Configuration storage instance.
     */
    // ConfigurationChangeException, really?
    public void register(ConfigurationStorage configurationStorage) throws ConfigurationChangeException {
        storageInstances.put(configurationStorage.type(), configurationStorage);

        Set<RootKey<?, ?>> storageRootKeys = rootKeys.values().stream().filter(
            rootKey -> configurationStorage.type() == rootKey.type()
        ).collect(Collectors.toSet());

        Data data;

        try {
            data = configurationStorage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        SuperRoot superRoot = new SuperRoot(rootCreator());

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(data.values());

        for (RootKey<?, ?> rootKey : storageRootKeys) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            InnerNode rootNode = createRootNode(rootKey);

            if (rootPrefixMap != null)
                fillFromPrefixMap(rootNode, rootPrefixMap);

            superRoot.addRoot(rootKey, rootNode);
        }

        StorageRoots storageRoots = new StorageRoots(superRoot, data.changeId());

        storagesRootsMap.put(configurationStorage.type(), storageRoots);

        configurationStorage.registerConfigurationListener(changedEntries -> updateFromListener(
            configurationStorage.type(),
            changedEntries
        ));
    }

    /**
     * Initializes the configuration storage - reads data and sets default values for missing configuration properties.
     * @param storageType Storage type.
     * @throws ConfigurationValidationException If configuration validation failed.
     * @throws ConfigurationChangeException If configuration framework failed to add default values and save them to
     *      storage.
     */
    public void initialize(ConfigurationType storageType) throws ConfigurationValidationException, ConfigurationChangeException {
        ConfigurationStorage configurationStorage = storageInstances.get(storageType);

        assert configurationStorage != null : storageType;

        try {
            ConfigurationSource defaultsCfgSource = new ConfigurationSource() {
                @Override public void descend(ConstructableTreeNode node) {
                    addDefaults((InnerNode)node);
                }
            };

            changeInternally(defaultsCfgSource, configurationStorage).get();
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof ConfigurationValidationException)
                throw (ConfigurationValidationException)cause;

            if (cause instanceof ConfigurationChangeException)
                throw (ConfigurationChangeException)cause;

            throw new ConfigurationChangeException(
                "Failed to write defalut configuration values into the storage " + configurationStorage.getClass(), e
            );
        }
        catch (InterruptedException e) {
            throw new ConfigurationChangeException(
                "Failed to initialize configuration storage " + configurationStorage.getClass(), e
            );
        }
    }

    /**
     * Changes the configuration.
     * @param source Configuration source to create patch from.
     * @param storage Expected storage for the changes. If null, a derived storage will be used
     * unconditionaly.
     * @return Future that is completed on change completion.
     */
    public CompletableFuture<Void> change(
        ConfigurationSource source,
        @Nullable ConfigurationStorage storage
    ) {
        Set<ConfigurationType> storagesTypes = new HashSet<>();

        ConstructableTreeNode collector = new ConstructableTreeNode() {
            @Override public void construct(String key, ConfigurationSource src) throws NoSuchElementException {
                RootKey<?, ?> rootKey = rootKeys.get(key);

                if (rootKey == null)
                    throw new NoSuchElementException(key);

                storagesTypes.add(rootKey.type());
            }

            @Override public ConstructableTreeNode copy() {
                throw new UnsupportedOperationException("copy");
            }
        };

        source.reset();

        source.descend(collector);

        assert !storagesTypes.isEmpty();

        if (storagesTypes.size() != 1) {
            return CompletableFuture.failedFuture(
                new ConfigurationChangeException(
                    "Cannot handle change request with configuration patches belonging to different storages."
                )
            );
        }

        ConfigurationStorage actualStorage = storageInstances.get(storagesTypes.iterator().next());

        if (storage != null && storage != actualStorage) {
            return CompletableFuture.failedFuture(
                new ConfigurationChangeException("Mismatched storage passed.")
            );
        }

        return changeInternally(source, actualStorage);
    }

    /** Stop component. */
    public void stop() {
        pool.shutdownNow();

        for (StorageRoots storageRoots : storagesRootsMap.values())
            storageRoots.changeFuture.completeExceptionally(new NodeStoppingException());
    }

    /**
     * Get root node by root key. Subject to revisiting.
     *
     * @param rootKey Root key.
     * @return Root node.
     */
    public InnerNode getRootNode(RootKey<?, ?> rootKey) {
        return storagesRootsMap.get(rootKey.type()).roots.getRoot(rootKey);
    }

    /**
     * @return Super root chat contains roots belonging to all storages.
     */
    public SuperRoot mergedSuperRoot() {
        return new SuperRoot(rootCreator(), storagesRootsMap.values().stream().map(roots -> roots.roots).collect(toList()));
    }

    /**
     * Internal configuration change method that completes provided future.
     *
     * @param src Configuration source.
     * @param storage Storage instance.
     * @return fut Future that will be completed after changes are written to the storage.
     */
    private CompletableFuture<Void> changeInternally(
        ConfigurationSource src,
        ConfigurationStorage storage
    ) {
        StorageRoots storageRoots = storagesRootsMap.get(storage.type());

        return CompletableFuture
            .supplyAsync(() -> {
                SuperRoot curRoots = storageRoots.roots;

                SuperRoot changes = curRoots.copy();

                src.reset();

                src.descend(changes);

                addDefaults(changes);

                Map<String, Serializable> allChanges = createFlattenedUpdatesMap(curRoots, changes);

                // Unlikely but still possible.
                if (allChanges.isEmpty())
                    return null;

                dropNulls(changes);

                List<ValidationIssue> validationIssues = ValidationUtil.validate(
                    curRoots,
                    changes,
                    this::getRootNode,
                    cachedAnnotations,
                    validators
                );

                if (!validationIssues.isEmpty())
                    throw new ConfigurationValidationException(validationIssues);

                return allChanges;
            }, pool)
            .thenCompose(allChanges -> {
                if (allChanges == null)
                    return completedFuture(null);

                return storage.write(allChanges, storageRoots.version)
                    .thenCompose(casResult -> {
                        if (casResult)
                            return storageRoots.changeFuture;
                        else
                            return storageRoots.changeFuture.thenCompose(v -> changeInternally(src, storage));
                    })
                    .exceptionally(throwable -> {
                        throw new ConfigurationChangeException("Failed to change configuration", throwable);
                    });
            });
    }

    /**
     * Updates configuration from storage listener.
     *
     * @param storageType Type of the storage that propagated these changes.
     * @param changedEntries Changed data.
     * @return Future that signifies update completion.
     */
    private CompletableFuture<Void> updateFromListener(
        ConfigurationType storageType,
        Data changedEntries
    ) {
        StorageRoots oldStorageRoots = this.storagesRootsMap.get(storageType);

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(changedEntries.values());

        compressDeletedEntries(dataValuesPrefixMap);

        SuperRoot oldSuperRoot = oldStorageRoots.roots;
        SuperRoot newSuperRoot = oldSuperRoot.copy();

        fillFromPrefixMap(newSuperRoot, dataValuesPrefixMap);

        long newChangeId = changedEntries.changeId();

        StorageRoots newStorageRoots = new StorageRoots(newSuperRoot, newChangeId);

        storagesRootsMap.put(storageType, newStorageRoots);

        return notificator.notify(oldSuperRoot, newSuperRoot, newChangeId)
            .whenComplete((v, t) -> {
                if (t == null)
                    oldStorageRoots.changeFuture.complete(null);
                else
                    oldStorageRoots.changeFuture.completeExceptionally(t);
            });
    }

    /**
     * "Compress" prefix map - this means that deleted named list elements will be represented as a single {@code null}
     * objects instead of a number of nullified configuration leaves.
     *
     * @param prefixMap Prefix map, constructed from the storage notification data or its subtree.
     */
    private void compressDeletedEntries(Map<String, ?> prefixMap) {
        // Here we basically assume that if prefix subtree contains single null child then all its childrens are nulls.
        // Replace all such elements will nulls, signifying that these are deleted named list elements.
        prefixMap.replaceAll((key, value) ->
            value instanceof Map && ((Map<?, ?>)value).containsValue(null) ? null : value
        );

        // Continue recursively.
        for (Object value : prefixMap.values()) {
            if (value instanceof Map)
                compressDeletedEntries((Map<String, ?>)value);
        }
    }
}
