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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.annotation.ConfigurationType;

/**
 * Test configuration storage.
 */
public class TestConfigurationStorage implements ConfigurationStorage {
    /** Configuration type.*/
    private final ConfigurationType configurationType;

    /** Map to store values. */
    private Map<String, Serializable> map = new HashMap<>();

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new CopyOnWriteArrayList<>();

    /** Storage version. */
    private AtomicLong version = new AtomicLong(0);

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
    public void fail(boolean fail) {
        this.fail = fail;
    }

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        if (fail)
            throw new StorageException("Failed to read data");

        return new Data(new HashMap<>(map), version.get());
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        if (fail)
            return CompletableFuture.failedFuture(new StorageException("Failed to write data"));

        if (sentVersion != version.get())
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            if (entry.getValue() != null)
                map.put(entry.getKey(), entry.getValue());
            else
                map.remove(entry.getKey());
        }

        version.incrementAndGet();

        listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, version.get())).join());

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public void registerConfigurationListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return configurationType;
    }
}
