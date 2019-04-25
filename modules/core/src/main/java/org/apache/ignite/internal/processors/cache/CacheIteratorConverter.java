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
 * @param <T> Type for iterator.
 * @param <V> Type for cache query future.
 */
public abstract class CacheIteratorConverter <T, V> {
    /**
     * Converts class V to class T.
     *
     * @param v Item to convert.
     * @return Converted item.
     */
    protected abstract T convert(V v);

    /**
     * Removes item.
     *
     * @param item Item to remove.
     */
    protected abstract void remove(T item);
}