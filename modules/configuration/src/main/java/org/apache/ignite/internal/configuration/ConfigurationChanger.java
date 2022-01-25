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

import static java.util.function.Function.identity;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.tree.InnerNode.INJECTED_NAME;
import static org.apache.ignite.internal.configuration.tree.InnerNode.INTERNAL_ID;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.KEY_SEPARATOR;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.compressDeletedEntries;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.dropNulls;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.escape;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.findEx;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.toPrefixMap;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.validation.MemberKey;
import org.apache.ignite.internal.configuration.validation.ValidationUtil;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public abstract class ConfigurationChanger implements DynamicConfigurationChanger {
    /** Thread pool. */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** Lazy annotations cache for configuration schema fields. */
    private final Map<MemberKey, Annotation[]> cachedAnnotations = new ConcurrentHashMap<>();

    /** Closure to execute when an update from the storage is received. */
    private final Notificator notificator;

    /** Root keys. Mapping: {@link RootKey#key()} -> identity (itself). */
    private final Map<String, RootKey<?, ?>> rootKeys;

    /** Validators. */
    private final Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators;

    /** Configuration storage. */
    private final ConfigurationStorage storage;

    /** Storage trees. */
    private volatile StorageRoots storageRoots;

    /**
     * Closure interface to be used by the configuration changer. An instance of this closure is passed into the constructor and invoked
     * every time when there's an update from any of the storages.
     */
    @FunctionalInterface
    public interface Notificator {
        /**
         * Invoked every time when the configuration is updated.
         *
         * @param oldRoot Old roots values. All these roots always belong to a single storage.
         * @param newRoot New values for the same roots as in {@code oldRoot}.
         * @param storageRevision Revision of the storage.
         * @return Future that must signify when processing is completed. Exceptional completion is not expected.
         */
        CompletableFuture<Void> notify(@Nullable SuperRoot oldRoot, SuperRoot newRoot, long storageRevision);
    }

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

        /**
         * Constructor.
         *
         * @param roots   Forest.
         * @param version Version associated with the currently known storage state.
         */
        private StorageRoots(SuperRoot roots, long version) {
            this.roots = roots;
            this.version = version;
        }
    }

    /**
     * Constructor.
     *
     * @param notificator Closure to execute when update from the storage is received.
     * @param rootKeys    Configuration root keys.
     * @param validators  Validators.
     * @param storage     Configuration storage.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type.
     */
    public ConfigurationChanger(
            Notificator notificator,
            Collection<RootKey<?, ?>> rootKeys,
            Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators,
            ConfigurationStorage storage
    ) {
        checkConfigurationType(rootKeys, storage);

        this.notificator = notificator;
        this.validators = validators;
        this.storage = storage;

        this.rootKeys = rootKeys.stream().collect(toMap(RootKey::key, identity()));
    }

    /**
     * Creates new {@code Node} object that corresponds to passed root keys root configuration node.
     *
     * @param rootKey Root key.
     * @return New {@link InnerNode} instance that represents root.
     */
    public abstract InnerNode createRootNode(RootKey<?, ?> rootKey);

    /**
     * Utility method to create {@link SuperRoot} parameter value.
     *
     * @return Function that creates root node by root name or returns {@code null} if root name is not found.
     */
    private Function<String, RootInnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : new RootInnerNode(rootKey, createRootNode(rootKey));
        };
    }

    /**
     * Start component.
     */
    // ConfigurationChangeException, really?
    public void start() throws ConfigurationChangeException {
        Data data;

        try {
            data = storage.readAll();
        } catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        SuperRoot superRoot = new SuperRoot(rootCreator());

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(data.values());

        for (RootKey<?, ?> rootKey : rootKeys.values()) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>) dataValuesPrefixMap.get(rootKey.key());

            InnerNode rootNode = createRootNode(rootKey);

            if (rootPrefixMap != null) {
                fillFromPrefixMap(rootNode, rootPrefixMap);
            }

            superRoot.addRoot(rootKey, rootNode);
        }

        //Workaround for distributed configuration.
        addDefaults(superRoot);

        storageRoots = new StorageRoots(superRoot, data.changeId());

        storage.registerConfigurationListener(this::updateFromListener);
    }

    /**
     * Initializes the configuration storage - reads data and sets default values for missing configuration properties.
     *
     * @throws ConfigurationValidationException If configuration validation failed.
     * @throws ConfigurationChangeException     If configuration framework failed to add default values and save them to storage.
     */
    public void initializeDefaults() throws ConfigurationValidationException, ConfigurationChangeException {
        try {
            ConfigurationSource defaultsCfgSource = new ConfigurationSource() {
                /** {@inheritDoc} */
                @Override
                public void descend(ConstructableTreeNode node) {
                    addDefaults((InnerNode) node);
                }
            };

            changeInternally(defaultsCfgSource).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof ConfigurationValidationException) {
                throw (ConfigurationValidationException) cause;
            }

            if (cause instanceof ConfigurationChangeException) {
                throw (ConfigurationChangeException) cause;
            }

            throw new ConfigurationChangeException(
                    "Failed to write default configuration values into the storage " + storage.getClass(), e
            );
        } catch (InterruptedException | TimeoutException e) {
            throw new ConfigurationChangeException(
                    "Failed to initialize configuration storage " + storage.getClass(), e
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> change(ConfigurationSource source) {
        return changeInternally(source);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T getLatest(List<KeyPathNode> path) {
        assert !path.isEmpty();
        assert path instanceof RandomAccess : path.getClass();
        assert !path.get(0).unresolvedName : path;

        // This map will be merged into the data from the storage. It's required for the conversion into tree to work.
        // Namely, named list order indexes and names are mandatory for conversion.
        Map<String, Map<String, Serializable>> extras = new HashMap<>();

        // Joiner for the prefix that will be used to fetch data from the storage.
        StringJoiner prefixJoiner = new StringJoiner(KEY_SEPARATOR);

        int pathSize = path.size();

        KeyPathNode lastPathNode = path.get(pathSize - 1);

        // This loop is required to accumulate prefix and resolve all unresolved named list elements' ids.
        for (int idx = 0; idx < pathSize; idx++) {
            KeyPathNode keyPathNode = path.get(idx);

            // Regular keys and resolved ids go straight to the prefix.
            if (!keyPathNode.unresolvedName) {
                // Fake name and 0 index go to extras in case of resolved named list elements.
                if (keyPathNode.namedListEntry) {
                    prefixJoiner.add(escape(keyPathNode.key));

                    String prefix = prefixJoiner + KEY_SEPARATOR;

                    extras.put(prefix, Map.of(
                            prefix + NamedListNode.NAME, "<name_placeholder>",
                            prefix + NamedListNode.ORDER_IDX, 0
                    ));
                } else {
                    prefixJoiner.add(keyPathNode.key);
                }

                continue;
            }

            assert keyPathNode.namedListEntry : path;

            // Here we have unresolved named list element. Name must be translated into the internal id.
            // There's a special path for this purpose in the storage.
            String unresolvedNameKey = prefixJoiner + KEY_SEPARATOR
                    + NamedListNode.IDS + KEY_SEPARATOR
                    + escape(keyPathNode.key);

            // Data from the storage.
            Serializable resolvedName = storage.readLatest(unresolvedNameKey);

            if (resolvedName == null) {
                throw new NoSuchElementException(prefixJoiner + KEY_SEPARATOR + escape(keyPathNode.key));
            }

            assert resolvedName instanceof String : resolvedName;

            // Resolved internal id from the map.
            String internalId = (String) resolvedName;

            // There's a chance that this is exactly what user wants. If their request ends with
            // `*.get("resourceName").internalId()` then the result can be returned straight away.
            if (idx == pathSize - 2 && INTERNAL_ID.equals(lastPathNode.key)) {
                assert !lastPathNode.unresolvedName : path;

                // Despite the fact that this cast looks very stupid, it is correct. Internal ids are always UUIDs.
                return (T) UUID.fromString(internalId);
            }

            prefixJoiner.add(internalId);

            String prefix = prefixJoiner + KEY_SEPARATOR;

            // Real name and 0 index go to extras in case of unresolved named list elements.
            extras.put(prefix, Map.of(
                    prefix + NamedListNode.NAME, keyPathNode.key,
                    prefix + NamedListNode.ORDER_IDX, 0
            ));
        }

        // Exceptional case, the only purpose of it is to ensure that named list element with given internal id does exist.
        // That id must be resolved, otherwise method would already be completed in the loop above.
        if (lastPathNode.key.equals(INTERNAL_ID) && !lastPathNode.unresolvedName && path.get(pathSize - 2).namedListEntry) {
            assert !path.get(pathSize - 2).unresolvedName : path;

            // Not very elegant, I know. <internal_id> is replaced with the <name> in the prefix.
            // <name> always exists in named list element, and it's an easy way to check element's existence.
            String nameStorageKey = prefixJoiner.toString().replaceAll(quote(INTERNAL_ID) + "$", NamedListNode.NAME);

            // Data from the storage.
            Serializable name = storage.readLatest(nameStorageKey);

            if (name != null) {
                // Id is already known.
                return (T) UUID.fromString(path.get(pathSize - 2).key);
            } else {
                throw new NoSuchElementException(prefixJoiner.toString());
            }
        }

        String prefix = prefixJoiner.toString();

        if (lastPathNode.key.equals(INTERNAL_ID) && !path.get(pathSize - 2).namedListEntry) {
            // This is not particularly efficient, but there's no way someone will actually use this case for real outside of tests.
            prefix = prefix.replaceAll(quote(KEY_SEPARATOR + INTERNAL_ID) + "$", "");
        } else if (lastPathNode.key.contains(INJECTED_NAME)) {
            prefix = prefix.replaceAll(quote(KEY_SEPARATOR + INJECTED_NAME), "");
        }

        // Data from the storage.
        Map<String, ? extends Serializable> storageData = storage.readAllLatest(prefix);

        // Data to be converted into the tree.
        Map<String, Serializable> mergedData = new HashMap<>();

        if (!storageData.isEmpty()) {
            mergedData.putAll(storageData);

            for (Entry<String, Map<String, Serializable>> extrasEntry : extras.entrySet()) {
                for (String storageKey : storageData.keySet()) {
                    String extrasPrefix = extrasEntry.getKey();

                    if (storageKey.startsWith(extrasPrefix)) {
                        // Add extra order indexes and names before converting it to the tree.
                        for (Entry<String, Serializable> extrasEntryMap : extrasEntry.getValue().entrySet()) {
                            mergedData.putIfAbsent(extrasEntryMap.getKey(), extrasEntryMap.getValue());
                        }

                        break;
                    }
                }
            }

            if (lastPathNode.namedListEntry) {
                // Change element's order index to zero. Conversion won't work if indexes range is not continuous.
                mergedData.put(prefix + KEY_SEPARATOR + NamedListNode.ORDER_IDX, 0);
            }
        }

        // Super root that'll be filled from the storage data.
        InnerNode rootNode = new SuperRoot(rootCreator());

        fillFromPrefixMap(rootNode, toPrefixMap(mergedData));

        // "addDefaults" won't work if regular root is missing.
        if (storageData.isEmpty()) {
            rootNode.construct(path.get(0).key, ConfigurationUtil.EMPTY_CFG_SRC, true);
        }

        addDefaults(rootNode);

        return findEx(path, rootNode);
    }

    /** Stop component. */
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(pool, 10, TimeUnit.SECONDS);

        StorageRoots roots = storageRoots;

        if (roots != null) {
            roots.changeFuture.completeExceptionally(new NodeStoppingException());
        }
    }

    /** {@inheritDoc} */
    @Override
    public InnerNode getRootNode(RootKey<?, ?> rootKey) {
        return storageRoots.roots.getRoot(rootKey);
    }

    /**
     * Get storage super root.
     *
     * @return Super root storage.
     */
    public SuperRoot superRoot() {
        return storageRoots.roots;
    }

    /**
     * Entry point for configuration changes.
     *
     * @param src Configuration source.
     * @return fut Future that will be completed after changes are written to the storage.
     */
    private CompletableFuture<Void> changeInternally(ConfigurationSource src) {
        StorageRoots localRoots = storageRoots;

        return storage.lastRevision()
            .thenCompose(storageRevision -> {
                assert storageRevision != null;

                if (localRoots.version < storageRevision) {
                    // Need to wait for the configuration updates from the storage, then try to update again (loop).
                    return localRoots.changeFuture.thenCompose(v -> changeInternally(src));
                } else {
                    return changeInternally0(localRoots, src);
                }
            })
            .exceptionally(throwable -> {
                Throwable cause = throwable.getCause();

                if (cause instanceof ConfigurationChangeException) {
                    throw ((ConfigurationChangeException) cause);
                } else {
                    throw new ConfigurationChangeException("Failed to change configuration", cause);
                }
            });
    }

    /**
     * Internal configuration change method that completes provided future.
     *
     * @param src Configuration source.
     * @return fut Future that will be completed after changes are written to the storage.
     */
    private CompletableFuture<Void> changeInternally0(StorageRoots localRoots, ConfigurationSource src) {
        return CompletableFuture
            .supplyAsync(() -> {
                SuperRoot curRoots = localRoots.roots;

                SuperRoot changes = curRoots.copy();

                src.reset();

                src.descend(changes);

                addDefaults(changes);

                Map<String, Serializable> allChanges = createFlattenedUpdatesMap(curRoots, changes);

                dropNulls(changes);

                List<ValidationIssue> validationIssues = ValidationUtil.validate(
                        curRoots,
                        changes,
                        this::getRootNode,
                        cachedAnnotations,
                        validators
                );

                if (!validationIssues.isEmpty()) {
                    throw new ConfigurationValidationException(validationIssues);
                }

                return allChanges;
            }, pool)
            .thenCompose(allChanges ->
                storage.write(allChanges, localRoots.version)
                    .thenCompose(casWroteSuccessfully -> {
                        if (casWroteSuccessfully) {
                            return localRoots.changeFuture;
                        } else {
                            // Here we go to next iteration of an implicit spin loop; we have to do it via recursion
                            // because we work with async code (futures).
                            return localRoots.changeFuture.thenCompose(v -> changeInternally(src));
                        }
                    })
            );
    }

    /**
     * Updates configuration from storage listener.
     *
     * @param changedEntries Changed data.
     * @return Future that signifies update completion.
     */
    private CompletableFuture<Void> updateFromListener(Data changedEntries) {
        StorageRoots oldStorageRoots = storageRoots;

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(changedEntries.values());

        compressDeletedEntries(dataValuesPrefixMap);

        SuperRoot oldSuperRoot = oldStorageRoots.roots;
        SuperRoot newSuperRoot = oldSuperRoot.copy();

        fillFromPrefixMap(newSuperRoot, dataValuesPrefixMap);

        long newChangeId = changedEntries.changeId();

        storageRoots = new StorageRoots(newSuperRoot, newChangeId);

        if (dataValuesPrefixMap.isEmpty()) {
            oldStorageRoots.changeFuture.complete(null);

            return CompletableFuture.completedFuture(null);
        } else {
            return notificator.notify(oldSuperRoot, newSuperRoot, newChangeId)
                .whenComplete((v, t) -> {
                    if (t == null) {
                        oldStorageRoots.changeFuture.complete(null);
                    } else {
                        oldStorageRoots.changeFuture.completeExceptionally(t);
                    }
                });
        }
    }

    /**
     * Notifies all listeners of the current configuration.
     *
     * @return Future that must signify when processing is completed.
     */
    CompletableFuture<Void> notifyCurrentConfigurationListeners() {
        StorageRoots storageRoots = this.storageRoots;

        return notificator.notify(null, storageRoots.roots, storageRoots.version);
    }
}
