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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.SuperRoot;
import org.apache.ignite.configuration.internal.validation.MemberKey;
import org.apache.ignite.configuration.internal.validation.ValidationUtil;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.nodeToFlatMap;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.patch;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.toPrefixMap;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public class ConfigurationChanger {
    /** */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** */
    private final Map<String, RootKey<?, ?>> rootKeys = new TreeMap<>();

    /** Map that has all the trees in accordance to their storages. */
    private final Map<Class<? extends ConfigurationStorage>, StorageRoots> storagesRootsMap = new ConcurrentHashMap<>();

    /** Annotation classes mapped to validator objects. */
    private Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = new HashMap<>();

    /**
     * Immutable data container to store version and all roots associated with the specific storage.
     */
    private static class StorageRoots {
        /** Immutable forest, so to say. */
        private final SuperRoot roots;

        /** Version associated with the currently known storage state. */
        private final long version;

        /** */
        private StorageRoots(SuperRoot roots, long version) {
            this.roots = roots;
            this.version = version;
        }
    }

    /** Lazy annotations cache for configuration schema fields. */
    private final Map<MemberKey, Annotation[]> cachedAnnotations = new ConcurrentHashMap<>();

    /** Storage instances by their classes. Comes in handy when all you have is {@link RootKey}. */
    private final Map<Class<? extends ConfigurationStorage>, ConfigurationStorage> storageInstances = new HashMap<>();

    /** Constructor. */
    public ConfigurationChanger(RootKey<?, ?>... rootKeys) {
        for (RootKey<?, ?> rootKey : rootKeys)
            this.rootKeys.put(rootKey.key(), rootKey);
    }

    /** */
    public <A extends Annotation> void addValidator(Class<A> annotationType, Validator<A, ?> validator) {
        validators
            .computeIfAbsent(annotationType, a -> new HashSet<>())
            .add(validator);
    }

    /** */
    public void addRootKey(RootKey<?, ?> rootKey) {
        assert !storageInstances.containsKey(rootKey.getStorageType());

        rootKeys.put(rootKey.key(), rootKey);
    }

    /**
     * Register changer.
     */
    // ConfigurationChangeException, really?
    public void register(ConfigurationStorage configurationStorage) throws ConfigurationChangeException {
        storageInstances.put(configurationStorage.getClass(), configurationStorage);

        Set<RootKey<?, ?>> storageRootKeys = rootKeys.values().stream().filter(
            rootKey -> configurationStorage.getClass() == rootKey.getStorageType()
        ).collect(Collectors.toSet());

        Data data;

        try {
            data = configurationStorage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        SuperRoot superRoot = new SuperRoot(rootKeys);

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(data.values());

        for (RootKey<?, ?> rootKey : storageRootKeys) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            InnerNode rootNode = rootKey.createRootNode();

            if (rootPrefixMap != null)
                fillFromPrefixMap(rootNode, rootPrefixMap);

            superRoot.addRoot(rootKey, rootNode);
        }

        StorageRoots storageRoots = new StorageRoots(superRoot, data.version());

        storagesRootsMap.put(configurationStorage.getClass(), storageRoots);

        configurationStorage.addListener(changedEntries -> updateFromListener(
            configurationStorage.getClass(),
            changedEntries
        ));
    }

    /** */
    public void initialize(Class<? extends ConfigurationStorage> storageType) {
        ConfigurationStorage configurationStorage = storageInstances.get(storageType);

        assert configurationStorage != null : storageType;

        StorageRoots storageRoots = storagesRootsMap.get(storageType);

        SuperRoot superRoot = storageRoots.roots;
        SuperRoot defaultsNode = new SuperRoot(rootKeys);

        addDefaults(superRoot, defaultsNode);

        List<ValidationIssue> validationIssues = ValidationUtil.validate(
            superRoot,
            defaultsNode,
            this::getRootNode,
            cachedAnnotations,
            validators
        );

        if (!validationIssues.isEmpty())
            throw new ConfigurationValidationException(validationIssues);

        try {
            change(defaultsNode, storageType).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ConfigurationChangeException(
                "Failed to write defalut configuration values into the storage " + configurationStorage.getClass(), e
            );
        }
    }

    /**
     * Get root node by root key. Subject to revisiting.
     *
     * @param rootKey Root key.
     */
    public InnerNode getRootNode(RootKey<?, ?> rootKey) {
        return storagesRootsMap.get(rootKey.getStorageType()).roots.getRoot(rootKey);
    }

    /**
     * Change configuration.
     * @param changes Map of changes by root key.
     */
    public CompletableFuture<Void> change(Map<RootKey<?, ?>, InnerNode> changes) {
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

        return change(new SuperRoot(rootKeys, changes), storagesTypes.iterator().next());
    }

    /** */
    public SuperRoot mergedSuperRoot() {
        SuperRoot mergedSuperRoot = new SuperRoot(rootKeys);

        for (StorageRoots storageRoots : storagesRootsMap.values())
            mergedSuperRoot.append(storageRoots.roots);

        return mergedSuperRoot;
    }

    /** */
    private CompletableFuture<Void> change(SuperRoot changes, Class<? extends ConfigurationStorage> storageType) {
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
        SuperRoot changes,
        ConfigurationStorage storage,
        CompletableFuture<?> fut
    ) {
        StorageRoots storageRoots = storagesRootsMap.get(storage.getClass());
        SuperRoot curRoots = storageRoots.roots;

        Map<String, Serializable> allChanges = new HashMap<>();

        //TODO IGNITE-14180 single putAll + remove matching value, this way "allChanges" will be fair.
        // These are changes explicitly provided by the client.
        allChanges.putAll(nodeToFlatMap(curRoots, changes));

        // It is necessary to reinitialize default values every time.
        // Possible use case that explicitly requires it: creation of the same named list entry with slightly
        // different set of values and different dynamic defaults at the same time.
        SuperRoot patchedSuperRoot = patch(curRoots, changes);

        SuperRoot defaultsNode = new SuperRoot(rootKeys);

        addDefaults(patchedSuperRoot, defaultsNode);

        // These are default values for non-initialized values, required to complete the configuration.
        allChanges.putAll(nodeToFlatMap(patchedSuperRoot, defaultsNode));

        // Unlikely but still possible.
        if (allChanges.isEmpty()) {
            fut.complete(null);

            return;
        }

        List<ValidationIssue> validationIssues = ValidationUtil.validate(
            storageRoots.roots,
            patch(patchedSuperRoot, defaultsNode),
            this::getRootNode,
            cachedAnnotations,
            validators
        );

        if (!validationIssues.isEmpty()) {
            fut.completeExceptionally(new ConfigurationValidationException(validationIssues));

            return;
        }

        CompletableFuture<Boolean> writeFut = storage.write(allChanges, storageRoots.version);

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

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(changedEntries.values());

        compressDeletedEntries(dataValuesPrefixMap);

        SuperRoot newSuperRoot = oldStorageRoots.roots.copy();

        fillFromPrefixMap(newSuperRoot, dataValuesPrefixMap);

        StorageRoots storageRoots = new StorageRoots(newSuperRoot, changedEntries.version());

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
}
