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
import java.util.function.Consumer;

/**
 * Storage interface for configuration.
 */
public interface ConfigurationStorage {
    /**
     * Save configuration property.
     *
     * @param propertyName Fully qualified name of the property.
     * @param object Object, that represents the value of the property.
     * @param <T> Type of the property.
     * @throws StorageException If failed to save object.
     */
    <T extends Serializable> void save(String propertyName, T object) throws StorageException;

    /**
     * Get property value from storage.
     *
     * @param propertyName Fully qualified name of the property.
     * @param <T> Type of the property.
     * @return Object, that represents the value of the property.
     * @throws StorageException If failed to retrieve object frm configuration storage.
     */
    <T extends Serializable> T get(String propertyName) throws StorageException;

    /**
     * Listen for the property change in the storage.
     *
     * @param key Key to listen on.
     * @param listener Listener function.
     * @param <T> Type of the property.
     * @throws StorageException If failed to attach listener to configuration storage.
     */
    <T extends Serializable> void listen(String key, Consumer<T> listener) throws StorageException;
}
