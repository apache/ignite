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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Bridge interface to access data storage in {@link DistributedMetaStorageImpl}.
 */
interface DistributedMetaStorageBridge {
    /**
     * Get data by key.
     *
     * @param globalKey The key.
     * @param unmarshal Whether the value should be unmarshalled or not.
     * @return Value associated with the key.
     * @throws IgniteCheckedException If reading or unmarshalling failed.
     */
    Serializable read(String globalKey, boolean unmarshal) throws IgniteCheckedException;

    /**
     * Iterate over all values corresponding to the keys with given prefix. It is guaranteed that iteration will be
     * executed in ascending keys order.
     *
     * @param globalKeyPrefix Prefix for the keys that will be iterated.
     * @param cb Callback that will be applied to all {@code <key, value>} pairs.
     * @throws IgniteCheckedException If reading or unmarshalling failed.
     */
    void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException;

    /**
     * Write data into storage.
     *
     * @param globalKey The key.
     * @param valBytes Value bytes.
     * @throws IgniteCheckedException If some IO problem occured.
     */
    void write(String globalKey, @Nullable byte[] valBytes) throws IgniteCheckedException;

    /**
     * Invoked when update message was received. Prepares storage to the writing of new value and notifies listeners
     * (optionally).
     *
     * @param histItem Update data.
     * @param val Unmarshalled value that needs to be written. This value is ignored if listeners shouldn't be notified.
     * @param notifyListeners Whether listeners should be notified about update or not.
     * @throws IgniteCheckedException If some IO or unmarshalling errors occured.
     */
    void onUpdateMessage(
        DistributedMetaStorageHistoryItem histItem,
        Serializable val,
        boolean notifyListeners
    ) throws IgniteCheckedException;

    /**
     * Remove information about the specific update from the history.
     *
     * @param ver Specific version for which the update information should be deleted.
     * @throws IgniteCheckedException If some IO error occured.
     */
    void removeHistoryItem(long ver) throws IgniteCheckedException;

    /**
     * Returns all {@code <key, value>} pairs currently stored in distributed metastorage. Values are not unmarshalled.
     *
     * @return Array of all keys and values.
     */
    DistributedMetaStorageHistoryItem[] localFullData() throws IgniteCheckedException;
}
