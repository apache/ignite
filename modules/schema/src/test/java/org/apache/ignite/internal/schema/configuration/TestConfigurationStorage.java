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

package org.apache.ignite.internal.schema.configuration;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.StorageException;

/**
 * Configuration storage for tests.
 */
public class TestConfigurationStorage implements ConfigurationStorage {
    /** */
    private final Set<ConfigurationStorageListener> listeners = new HashSet<>();

    /** {@inheritDoc} */
    @Override public Data readAll() throws StorageException {
        return new Data(Collections.emptyMap(), 0);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long version) {
        for (ConfigurationStorageListener listener : listeners)
            listener.onEntriesChanged(new Data(newValues, version + 1));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public void registerConfigurationListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    @Override public void notifyApplied(long storageRevision) {
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }
}
