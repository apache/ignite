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

package org.apache.ignite.cache.eviction;

import org.jetbrains.annotations.*;

import javax.cache.*;

/**
 * Evictable cache entry passed into {@link CacheEvictionPolicy}.
 *
 * @author @java.author
 * @version @java.version
 */
public interface EvictableEntry<K, V> extends Cache.Entry<K, V> {
    public boolean evict();

    public boolean isCached();

    /**
     * Gets metadata by name.
     *
     * @return Metadata value or {@code null}.
     */
    @Nullable public <T> T meta();

    /**
     * Adds a new metadata.
     *
     * @param val Metadata value.
     * @return Metadata previously associated with given name, or
     *      {@code null} if there was none.
     */
    @Nullable public <T> T addMeta(T val);

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param val Value to add if it's not attached already.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @Nullable public <T> T putMetaIfAbsent(T val);

    /**
     * Replaces given metadata with new {@code newVal} value only if its current value
     * is equal to {@code curVal}. Otherwise, it is no-op.
     *
     * @param curVal Current value to check.
     * @param newVal New value.
     * @return {@code true} if replacement occurred, {@code false} otherwise.
     */
    public <T> boolean replaceMeta(T curVal, T newVal);

    /**
     * Removes metadata by name.
     *
     * @return Value of removed metadata or {@code null}.
     */
    @Nullable public <T> T removeMeta();

    /**
     * Removes metadata only if its current value is equal to {@code val} passed in.
     *
     * @param val Value to compare.
     * @return {@code True} if value was removed, {@code false} otherwise.
     */
    public <T> boolean removeMeta(T val);
}
