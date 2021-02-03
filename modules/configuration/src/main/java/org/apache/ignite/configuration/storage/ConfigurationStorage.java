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
package org.apache.ignite.configuration.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Common interface for configuration storage.
 */
public interface ConfigurationStorage {
    /**
     * Read all configuration values and current storage version.
     * @return Values and version.
     * @throws StorageException If failed to retrieve data.
     */
    Data readAll() throws StorageException;

    /**
     * Write key-value pairs into the storage with last known version.
     * @param newValues Key-value pairs.
     * @param version Last known version.
     * @return {@code true} if successfully written, {@code false} if version of the storage is different from the passed
     * argument.
     * @throws StorageException If failed to write data.
     */
    boolean write(Map<String, Serializable> newValues, int version) throws StorageException;

    /**
     * Get all the keys of the configuration storage.
     * @return Set of keys.
     * @throws StorageException If failed to retrieve keys.
     */
    Set<String> keys() throws StorageException;

    /**
     * Add listener to the storage that notifies of data changes..
     * @param listener Listener.
     */
    void addListener(ConfigurationStorageListener listener);

    /**
     * Remove storage listener.
     * @param listener Listener.
     */
    void removeListener(ConfigurationStorageListener listener);
}
