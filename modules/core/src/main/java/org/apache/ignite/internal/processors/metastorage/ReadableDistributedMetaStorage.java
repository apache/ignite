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

package org.apache.ignite.internal.processors.metastorage;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * API for distributed data storage. Gives the ability to store configuration data (or any other data)
 * consistently and cluster-wide. It is guaranteed that every read value is the same on every node in the cluster
 * all the time.
 */
public interface ReadableDistributedMetaStorage {
    /**
     * Get the total number of updates (write/remove) that metastorage ever had.
     */
    long getUpdatesCount();

    /**
     * Get value by the key. Should be consistent for all nodes.
     *
     * @param key The key.
     * @return Value associated with the key.
     * @throws IgniteCheckedException If reading or unmarshalling failed.
     */
    @Nullable <T extends Serializable> T read(@NotNull String key) throws IgniteCheckedException;

    /**
     * Iterate over all values corresponding to the keys with given prefix. It is guaranteed that iteration will be
     * executed in ascending keys order.
     *
     * @param keyPrefix Prefix for the keys that will be iterated.
     * @param cb Callback that will be applied to all {@code <key, value>} pairs.
     * @throws IgniteCheckedException If reading or unmarshalling failed.
     */
    void iterate(
        @NotNull String keyPrefix,
        @NotNull BiConsumer<String, ? super Serializable> cb
    ) throws IgniteCheckedException;

    /**
     * Add listener on data updates. Updates happens it two cases:
     * <ul>
     *     <li>
     *         Some node invoked write or remove. Listeners are invoked after update update operation is
     *         already completed.
     *     </li>
     *     <li>
     *         Node is just started and not ready for write yet. In this case listeners are invoked for every
     *         key with new value (retrieved from the clueter) or already existing value if there was no updates
     *         for given key. This guarantees that all listeners are invoked for all updates in case of failover.
     *     </li>
     * </ul>
     *
     * @param keyPred Predicate to check whether this listener should be invoked on given key update or not.
     * @param lsnr Listener object.
     * @see DistributedMetaStorageListener
     */
    void listen(@NotNull Predicate<String> keyPred, DistributedMetaStorageListener<?> lsnr);
}
