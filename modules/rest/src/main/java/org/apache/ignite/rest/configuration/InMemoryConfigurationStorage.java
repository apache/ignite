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
package org.apache.ignite.rest.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;

/**
 * Temporary configuration storage.
 */
public class InMemoryConfigurationStorage implements ConfigurationStorage {
    /** Map to store values. */
    private Map<String, Serializable> map = new ConcurrentHashMap<>();

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new ArrayList<>();

    /** Storage version. */
    private AtomicLong version = new AtomicLong(0);

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        return new Data(new HashMap<>(map), version.get());
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) throws StorageException {
        if (sentVersion != version.get())
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            if (entry.getValue() != null)
                map.put(entry.getKey(), entry.getValue());
            else
                map.remove(entry.getKey());
        }

        version.incrementAndGet();

        listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, version.get())));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized Set<String> keys() throws StorageException {
        return map.keySet();
    }

    /** {@inheritDoc} */
    @Override public void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);
    }
}
