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
package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import java.io.Serializable;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
public interface ReadOnlyMetastorage {
    /** */
    Serializable read(String key) throws IgniteCheckedException;

    /** */
    byte[] readRaw(String key) throws IgniteCheckedException;

    /**
     * Read all key/value pairs where key has provided prefix.
     * It is guaranteed that callback will be applied to matching keys in ascending order.
     *
     * @param keyPrefix Key prefix.
     * @param cb Callback to invoke on each matching key/value pair.
     * @param unmarshal {@code True} if object passed into {@code cb} should be unmarshalled.
     * @throws IgniteCheckedException If failed.
     */
    public void iterate(
        String keyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException;
}
