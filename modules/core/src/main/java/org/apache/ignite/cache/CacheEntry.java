/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache;

import java.util.Set;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;

/**
 * Cache entry that extends {@link javax.cache.Cache.Entry} by providing additional entry related information.
 * <p>
 * To get an instance of {@code CacheEntry} from {@link javax.cache.Cache.Entry} use
 * {@link javax.cache.Cache.Entry#unwrap(Class)} method by passing {@code CacheEntry} class to it as the argument.
 * <p>
 * {@code CacheEntry} is supported only for {@link javax.cache.Cache.Entry} returned by one of the following methods:
 * <ul>
 * <li>{@link javax.cache.Cache#invoke(Object, EntryProcessor, Object...)}</li>
 * <li>{@link javax.cache.Cache#invokeAll(Set, EntryProcessor, Object...)}</li>
 * <li>invoke and invokeAll methods of {@link IgniteCache}</li>
 * </ul>
 * <p>
 * To get an instance of {@code CacheEntry} directly use {@link IgniteCache#getEntry(Object)} or
 * {@link IgniteCache#getEntries(Set)} methods.
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
}