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

package org.apache.ignite.cache;

import java.util.Set;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;

/**
 * Cache entry that extends {@link javax.cache.Cache.Entry} by providing additional entry related information.
 *
 * To get an instance of {@code CacheEntry} use {@link javax.cache.Cache.Entry#unwrap(Class)} method by passing
 * {@code CacheEntry} class to it as the argument.
 * <p>
 * {@code CacheEntry} is supported only for {@link javax.cache.Cache.Entry} returned by one of the following methods:
 * <ul>
 * <li>{@link javax.cache.Cache#invoke(Object, EntryProcessor, Object...)}</li>
 * <li>{@link javax.cache.Cache#invokeAll(Set, EntryProcessor, Object...)}</li>
 * <li>invoke and invokeAll methods of {@link IgniteCache}</li>
 * <li>{@link IgniteCache#randomEntry()}</li>
 * </ul>
 * <p>
 * {@code CacheEntry} is not supported for {@link javax.cache.Cache#iterator()} because of performance reasons.
 * {@link javax.cache.Cache#iterator()} loads entries from all the cluster nodes and to speed up the load additional
 * information, like entry's version, is ignored.
 *
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * IgniteCache<Integer, String> cache = grid(0).cache(null);
 *
 * CacheEntry<String, Integer> entry1 = cache.invoke(100,
 *     new EntryProcessor<Integer, String, CacheEntry<String, Integer>>() {
 *          public CacheEntry<String, Integer> process(MutableEntry<Integer, String> entry,
 *              Object... arguments) throws EntryProcessorException {
 *                  return entry.unwrap(CacheEntry.class);
 *          }
 *     });
 *
 * // Cache entry for the given key may be updated at some point later.
 *
 * CacheEntry<String, Integer> entry2 = cache.invoke(100,
 *     new EntryProcessor<Integer, String, CacheEntry<String, Integer>>() {
 *          public CacheEntry<String, Integer> process(MutableEntry<Integer, String> entry,
 *              Object... arguments) throws EntryProcessorException {
 *                  return entry.unwrap(CacheEntry.class);
 *          }
 *     });
 *
 * // Comparing entries' versions.
 * if (entry1.version().compareTo(entry2.version()) < 0) {
 *     // the entry has been updated
 * }
 * </pre>
 */
public interface CacheEntry<K, V> extends Cache.Entry<K, V> {
    /**
     * Returns a comparable object representing the version of this cache entry.
     * <p>
     * It is valid to compare cache entries' versions for the same key. In this case the latter update will be
     * represented by a higher version. The result of versions comparison of cache entries of different keys is
     * undefined.
     *
     * @return Version of this cache entry.
     */
    public Comparable version();

    /**
     * Returns the time when the cache entry for the given key has been updated or initially created.
     * <p>
     * It is valid to compare cache entries' update time for the same key. In this case the latter update will
     * be represented by higher update time. The result of update time comparison of cache entries of different keys is
     * undefined.
     *
     * @return Time in milliseconds.
     */
    public long updateTime();
}