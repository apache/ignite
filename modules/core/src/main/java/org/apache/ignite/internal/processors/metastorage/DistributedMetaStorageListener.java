/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.metastorage;

import java.io.Serializable;
import java.util.function.Predicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for distributed metastorage data updates.
 *
 * @see ReadableDistributedMetaStorage#listen(Predicate, DistributedMetaStorageListener)
 */
@FunctionalInterface
public interface DistributedMetaStorageListener<T extends Serializable> {
    /**
     * Invoked in two cases:
     * <ul>
     *     <li>data was dynamicaly updated;</li>
     *     <li>node was started. In this case {@code oldVal} and {@code newVal} might be different only if new data
     *     was received from cluster</li>
     * </ul>
     *
     * @param key The key.
     * @param oldVal Previous value associated with the key.
     * @param newVal New value after update.
     */
    void onUpdate(@NotNull String key, @Nullable T oldVal, @Nullable T newVal);
}
