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

package org.apache.ignite.internal.processors.cache;

/**
 *
 */
public interface KeyCacheObject extends CacheObject {
    /**
     * @return Key hash code.
     */
    @Override public int hashCode();

    /**
     * @return {@code True} if internal cache key.
     */
    public boolean internal();

    /**
     * @return Partition ID for this key or -1 if it is unknown.
     */
    public int partition();

    /**
     * Sets partition ID for this key.
     *
     * @param part Partition ID.
     */
    public void partition(int part);

    /**
     * @param part Partition ID.
     * @return Copy of this object with given partition set.
     */
    public KeyCacheObject copy(int part);
}
