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
package org.apache.ignite.internal.configuration.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toUnmodifiableMap;

/**
 * Test configuration storage.
 */
public class TestConfigurationStorage implements ConfigurationStorage {
    /** Configuration type.*/
    private final ConfigurationType configurationType;

    /** Map to store values. */
    private final Map<String, Serializable> map = new HashMap<>();

    /** Change listeners. */
    private final List<ConfigurationStorageListener> listeners = new ArrayList<>();

    /** Storage version. */
    private long version = 0;

    /** Should fail on every operation. */
    private boolean fail = false;

    /**
     * Constructor.
     *
     * @param type Configuration type.
     */
    public TestConfigurationStorage(ConfigurationType type) {
        configurationType = type;
    }

    /**
     * Set fail flag.
     * @param fail Fail flag.
     */
    public synchronized void fail(boolean fail) {
        this.fail = fail;
    }

    /** {@inheritDoc} */
    @Override public synchronized Map<String, Serializable> readAllLatest(String prefix) throws StorageException {
        if (fail)
            throw new StorageException("Failed to read data");

        return map.entrySet().stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        if (fail)
            throw new StorageException("Failed to read data");

        return new Data(new HashMap<>(map), version);
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(
        Map<String, ? extends Serializable> newValues, long sentVersion
    ) {
        if (fail)
            return CompletableFuture.failedFuture(new StorageException("Failed to write data"));

        if (sentVersion != version)
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, ? extends Serializable> entry : newValues.entrySet()) {
            if (entry.getValue() != null)
                map.put(entry.getKey(), entry.getValue());
            else
                map.remove(entry.getKey());
        }

        version++;

        var data = new Data(newValues, version);

        listeners.forEach(listener -> listener.onEntriesChanged(data).join());

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized void registerConfigurationListener(@NotNull ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return configurationType;
    }
}
