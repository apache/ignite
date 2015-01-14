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

package org.apache.ignite.streamer.index;


import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Streamer index entry. Individual index entry contains index key, value, and all events
 * associated with given key.
 *
 */
public interface StreamerIndexEntry<E, K, V> {
    /**
     * Gets events associated with given index key and value.
     * <p>
     * Events are tracked only if {@link StreamerIndexProvider#getPolicy()}
     * is set to {@link StreamerIndexPolicy#EVENT_TRACKING_ON} or
     * {@link StreamerIndexPolicy#EVENT_TRACKING_ON_DEDUP}.
     *
     * @return Events associated with given index key and value or {@code null} if event tracking is off.
     */
    @Nullable public Collection<E> events();

    /**
     * Gets index entry key.
     *
     * @return Index entry key.
     */
    public K key();

    /**
     * Gets index entry value.
     * <p>
     * For sorted indexes, the sorting happens based on this value.
     *
     * @return Index entry value.
     */
    public V value();
}
