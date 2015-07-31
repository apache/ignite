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

package org.apache.ignite.cache.version;

import org.apache.ignite.*;

import javax.cache.*;
import javax.cache.processor.*;
import java.util.*;

/**
 * Cache entry that stores entry's version related information.
 *
 * To get a {@code VersionedEntry} of an {@link Cache.Entry} use {@link Cache.Entry#unwrap(Class)} by passing
 * {@code VersionedEntry} class to it as the argument.
 * <p>
 * {@code VersionedEntry} is supported only for {@link Cache.Entry} returned by one of the following methods:
 * <ul>
 * <li>{@link Cache#invoke(Object, EntryProcessor, Object...)}</li>
 * <li>{@link Cache#invokeAll(Set, EntryProcessor, Object...)}</li>
 * <li>invoke and invokeAll methods of {@link IgniteCache}</li>
 * <li>{@link IgniteCache#randomEntry()}</li>
 * </ul>
 * <p>
 * {@code VersionedEntry} is not supported for {@link Cache#iterator()} because of performance reasons.
 * {@link Cache#iterator()} loads entries from all the cluster nodes and to speed up the load version information
 * is excluded from responses.
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * Cache<Integer, String> cache = grid(0).cache(null);
 *
 *  cache.invoke(100, new EntryProcessor<Integer, String, Object>() {
 *      public Object process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
 *          VersionedEntry<Integer, String> verEntry = entry.unwrap(VersionedEntry.class);
 *          return entry;
 *       }
 *   });
 * </pre>
 */
public interface VersionedEntry<K, V> extends Cache.Entry<K, V> {
    /**
     * Versions comparator.
     */
    public static final Comparator<VersionedEntry> VERSIONS_COMPARATOR = new Comparator<VersionedEntry>() {
        @Override public int compare(VersionedEntry o1, VersionedEntry o2) {
            int res = Integer.compare(o1.topologyVersion(), o2.topologyVersion());

            if (res != 0)
                return res;

            res = Long.compare(o1.order(), o2.order());

            if (res != 0)
                return res;

            return Integer.compare(o1.nodeOrder(), o2.nodeOrder());
        }
    };

    /**
     * Gets entry's topology version.
     *
     * @return Topology version plus number of seconds from the start time of the first grid node.
     */
    public int topologyVersion();

    /**
     * Gets entry's order.
     *
     * @return Version order.
     */
    public long order();

    /**
     * Gets entry's node order.
     *
     * @return Node order on which this version was assigned.
     */
    public int nodeOrder();

    /**
     * Gets entry's global time.
     *
     * @return Adjusted time.
     */
    public long globalTime();
}
