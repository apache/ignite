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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.util.ConfigurationUtil;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;

import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.nodeToFlatMap;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public class ConfigurationChanger {
    /** */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** Map of configurations' configurators. */
    @Deprecated
    private final Map<RootKey<?>, Configurator<?>> configurators = new HashMap<>();

    /** */
    private final Set<RootKey<?>> rootKeys = new HashSet<>();

    /** Map that has all the trees in accordance to their storages. */
    private final Map<Class<? extends ConfigurationStorage>, StorageRoots> storagesRootsMap = new ConcurrentHashMap<>();

    /**
     * Immutable data container to store version and all roots associated with the specific storage.
     */
    public static class StorageRoots {
        /** Immutable forest, so to say. */
        private final Map<RootKey<?>, InnerNode> roots;

        /** Version associated with the currently known storage state. */
        private final long version;

        /** */
        private StorageRoots(Map<RootKey<?>, InnerNode> roots, long version) {
            this.roots = Collections.unmodifiableMap(roots);
            this.version = version;
        }
    }

    /** Storage instances by their classes. Comes in handy when all you have is {@link RootKey}. */
    private final Map<Class<? extends ConfigurationStorage>, ConfigurationStorage> storageInstances = new HashMap<>();

    /** Constructor. */
    public ConfigurationChanger(RootKey<?>... rootKeys) {
        this.rootKeys.addAll(Arrays.asList(rootKeys));
    }

    /** */
    public void addRootKey(RootKey<?> rootKey) {
        assert !storageInstances.containsKey(rootKey.getStorageType());

        rootKeys.add(rootKey);
    }

    /**
     * Initialize changer.
     */
    // ConfigurationChangeException, really?
    public void init(ConfigurationStorage configurationStorage) throws ConfigurationChangeException {
        storageInstances.put(configurationStorage.getClass(), configurationStorage);

        Set<RootKey<?>> storageRootKeys = rootKeys.stream().filter(
            rootKey -> configurationStorage.getClass() == rootKey.getStorageType()
        ).collect(Collectors.toSet());

        Data data;

        try {
            data = configurationStorage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        Map<RootKey<?>, InnerNode> storageRootsMap = new HashMap<>();

        Map<String, ?> dataValuesPrefixMap = ConfigurationUtil.toPrefixMap(data.values());

        for (RootKey<?> rootKey : storageRootKeys) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            if (rootPrefixMap == null) {
                //TODO IGNITE-14193 Init with defaults.
                storageRootsMap.put(rootKey, rootKey.createRootNode());
            }
            else {
                InnerNode rootNode = rootKey.createRootNode();

                ConfigurationUtil.fillFromPrefixMap(rootNode, rootPrefixMap);

                storageRootsMap.put(rootKey, rootNode);
            }
        }

        storagesRootsMap.put(configurationStorage.getClass(), new StorageRoots(storageRootsMap, data.version()));

        configurationStorage.addListener(changedEntries -> updateFromListener(
            configurationStorage.getClass(),
            changedEntries
        ));
    }

    /**
     * Add configurator.
     * @param key Root configuration key of the configurator.
     * @param configurator Configuration's configurator.
     */
    //TODO IGNITE-14183 Refactor, get rid of configurator and create some "validator".
    @Deprecated
    public void registerConfiguration(RootKey<?> key, Configurator<?> configurator) {
        configurators.put(key, configurator);
    }

    /**
     * Get root node by root key. Subject to revisiting.
     *
     * @param rootKey Root key.
     */
    public TraversableTreeNode getRootNode(RootKey<?> rootKey) {
        return this.storagesRootsMap.get(rootKey.getStorageType()).roots.get(rootKey);
    }

    /**
     * Change configuration.
     * @param changes Map of changes by root key.
     */
    public CompletableFuture<Void> change(Map<RootKey<?>, TraversableTreeNode> changes) {
        if (changes.isEmpty())
            return CompletableFuture.completedFuture(null);

        Set<Class<? extends ConfigurationStorage>> storagesTypes = changes.keySet().stream()
            .map(RootKey::getStorageType)
            .collect(Collectors.toSet());

        assert !storagesTypes.isEmpty();

        if (storagesTypes.size() != 1) {
            return CompletableFuture.failedFuture(
                new ConfigurationChangeException("Cannot change configurations belonging to different storages.")
            );
        }

        Class<? extends ConfigurationStorage> storageType = storagesTypes.iterator().next();

        ConfigurationStorage storage = storageInstances.get(storageType);

        CompletableFuture<Void> fut = new CompletableFuture<>();

        pool.execute(() -> change0(changes, storage, fut));

        return fut;
    }

    /**
     * Internal configuration change method that completes provided future.
     * @param changes Map of changes by root key.
     * @param storage Storage instance.
     * @param fut Future, that must be completed after changes are written to the storage.
     */
    private void change0(
        Map<RootKey<?>, TraversableTreeNode> changes,
        ConfigurationStorage storage,
        CompletableFuture<?> fut
    ) {
        Map<String, Serializable> allChanges = new HashMap<>();

        for (Map.Entry<RootKey<?>, TraversableTreeNode> change : changes.entrySet())
            allChanges.putAll(nodeToFlatMap(change.getKey(), getRootNode(change.getKey()), change.getValue()));

        StorageRoots roots = storagesRootsMap.get(storage.getClass());

        ValidationResult validationResult = validate(roots, changes);

        List<ValidationIssue> validationIssues = validationResult.issues();

        if (!validationIssues.isEmpty()) {
            fut.completeExceptionally(new ConfigurationValidationException(validationIssues));

            return;
        }

        CompletableFuture<Boolean> writeFut = storage.write(allChanges, roots.version);

        writeFut.whenCompleteAsync((casResult, throwable) -> {
            if (throwable != null)
                fut.completeExceptionally(new ConfigurationChangeException("Failed to change configuration", throwable));
            else if (casResult)
                fut.complete(null);
            else
                change0(changes, storage, fut);
        }, pool);
    }

    /**
     * Update configuration from storage listener.
     * @param storageType Type of the storage that propagated these changes.
     * @param changedEntries Changed data.
     */
    private void updateFromListener(
        Class<? extends ConfigurationStorage> storageType,
        Data changedEntries
    ) {
        StorageRoots oldStorageRoots = this.storagesRootsMap.get(storageType);

        Map<RootKey<?>, InnerNode> storageRootsMap = new HashMap<>(oldStorageRoots.roots);

        Map<String, ?> dataValuesPrefixMap = ConfigurationUtil.toPrefixMap(changedEntries.values());

        compressDeletedEntries(dataValuesPrefixMap);

        for (RootKey<?> rootKey : oldStorageRoots.roots.keySet()) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            if (rootPrefixMap != null) {
                InnerNode rootNode = oldStorageRoots.roots.get(rootKey).copy();

                ConfigurationUtil.fillFromPrefixMap(rootNode, rootPrefixMap);

                storageRootsMap.put(rootKey, rootNode);
            }
        }

        StorageRoots storageRoots = new StorageRoots(storageRootsMap, changedEntries.version());

        storagesRootsMap.put(storageType, storageRoots);

        //TODO IGNITE-14180 Notify listeners.
    }

    /**
     * "Compress" prefix map - this means that deleted named list elements will be represented as a single {@code null}
     * objects instead of a number of nullified configuration leaves.
     *
     * @param prefixMap Prefix map, constructed from the storage notification data or its subtree.
     */
    private void compressDeletedEntries(Map<String, ?> prefixMap) {
        // Here we basically assume that if prefix subtree contains single null child then all its childrens are nulls.
        Set<String> keysForRemoval = prefixMap.entrySet().stream()
            .filter(entry ->
                entry.getValue() instanceof Map && ((Map<?, ?>)entry.getValue()).containsValue(null)
            )
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        // Replace all such elements will nulls, signifying that these are deleted named list elements.
        for (String key : keysForRemoval)
            prefixMap.put(key, null);

        // Continue recursively.
        for (Object value : prefixMap.values()) {
            if (value instanceof Map)
                compressDeletedEntries((Map<String, ?>)value);
        }
    }

    /**
     * Validate configuration changes.
     *
     * @param storageRoots Storage roots.
     * @param changes Configuration changes.
     * @return Validation results.
     */
    // Will be used in the future, I promise (IGNITE-14183).
    @SuppressWarnings("unused")
    private ValidationResult validate(
        StorageRoots storageRoots,
        Map<RootKey<?>, TraversableTreeNode> changes
    ) {
        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?>, TraversableTreeNode> entry : changes.entrySet()) {
            RootKey<?> rootKey = entry.getKey();
            TraversableTreeNode changesForRoot = entry.getValue();

            final Configurator<?> configurator = configurators.get(rootKey);

            // TODO IGNITE-14183 This will be fixed later
            if (configurator != null) {
                List<ValidationIssue> list = configurator.validateChanges(changesForRoot);
                issues.addAll(list);
            }
        }

        return new ValidationResult(issues);
    }

    /**
     * Results of the validation.
     */
    private static final class ValidationResult {
        /** List of issues. */
        private final List<ValidationIssue> issues;

        /**
         * Constructor.
         * @param issues List of issues.
         */
        private ValidationResult(List<ValidationIssue> issues) {
            this.issues = issues;
        }

        /**
         * Get issues.
         * @return Issues.
         */
        public List<ValidationIssue> issues() {
            return issues;
        }
    }
}
