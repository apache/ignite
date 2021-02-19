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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
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
    private Map<RootKey<?>, Configurator<?>> registry = new HashMap<>();

    /** Storage. */
    private ConfigurationStorage configurationStorage;

    /** Changer's last known version of storage. */
    private final AtomicInteger version = new AtomicInteger(0);

    /** Constructor. */
    public ConfigurationChanger(ConfigurationStorage configurationStorage) {
        this.configurationStorage = configurationStorage;
    }

    /**
     * Initialize changer.
     */
    public void init() throws ConfigurationChangeException {
        final Data data;

        try {
            data = configurationStorage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        version.set(data.version());

        configurationStorage.addListener(this::updateFromListener);

        // TODO: IGNITE-14118 iterate over data and fill Configurators
    }

    /**
     * Add configurator.
     * @param key Root configuration key of the configurator.
     * @param configurator Configuration's configurator.
     */
    public void registerConfiguration(RootKey<?> key, Configurator<?> configurator) {
        registry.put(key, configurator);
    }

    /**
     * Change configuration.
     * @param changes Map of changes by root key.
     */
    public CompletableFuture<Void> change(Map<RootKey<?>, TraversableTreeNode> changes) {
        CompletableFuture<Void> fut = new CompletableFuture<>();

        pool.execute(() -> change0(changes, fut));

        return fut;
    }

    /**
     * Internal configuration change method that completes provided future.
     * @param changes Map of changes by root key.
     * @param fut Future, that must be completed after changes are written to the storage.
     */
    private void change0(Map<RootKey<?>, TraversableTreeNode> changes, CompletableFuture<?> fut) {
        Map<String, Serializable> allChanges = changes.entrySet().stream()
            .map((Map.Entry<RootKey<?>, TraversableTreeNode> change) -> nodeToFlatMap(change.getKey(), change.getValue()))
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final ValidationResult validationResult = validate(changes);

        List<ValidationIssue> validationIssues = validationResult.issues();

        if (!validationIssues.isEmpty()) {
            fut.completeExceptionally(new ConfigurationValidationException(validationIssues));

            return;
        }

        final int version = validationResult.version();

        CompletableFuture<Boolean> writeFut = configurationStorage.write(allChanges, version);

        writeFut.whenCompleteAsync((casResult, throwable) -> {
            if (throwable != null)
                fut.completeExceptionally(new ConfigurationChangeException("Failed to change configuration", throwable));
            else if (casResult)
                fut.complete(null);
            else
                change0(changes, fut);
        }, pool);
    }

    /**
     * Update configuration from storage listener.
     * @param changedEntries Changed data.
     */
    private synchronized void updateFromListener(Data changedEntries) {
        // TODO: IGNITE-14118 add tree update
        version.set(changedEntries.version());
    }

    /**
     * Validate configuration changes.
     * @param changes Configuration changes.
     * @return Validation results.
     */
    private synchronized ValidationResult validate(Map<RootKey<?>, TraversableTreeNode> changes) {
        final int version = this.version.get();

        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?>, TraversableTreeNode> entry : changes.entrySet()) {
            RootKey<?> rootKey = entry.getKey();
            TraversableTreeNode changesForRoot = entry.getValue();

            final Configurator<?> configurator = registry.get(rootKey);

            List<ValidationIssue> list = configurator.validateChanges(changesForRoot);
            issues.addAll(list);
        }

        return new ValidationResult(issues, version);
    }

    /**
     * Results of the validation.
     */
    private static final class ValidationResult {
        /** List of issues. */
        private final List<ValidationIssue> issues;

        /** Version of configuration that changes were validated against. */
        private final int version;

        /**
         * Constructor.
         * @param issues List of issues.
         * @param version Version.
         */
        private ValidationResult(List<ValidationIssue> issues, int version) {
            this.issues = issues;
            this.version = version;
        }

        /**
         * Get issues.
         * @return Issues.
         */
        public List<ValidationIssue> issues() {
            return issues;
        }

        /**
         * Get version.
         * @return Version.
         */
        public int version() {
            return version;
        }
    }
}
