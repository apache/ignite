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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;

import javax.cache.event.*;

/**
 *
 */
public class CacheEntryEvent<K, V> extends javax.cache.event.CacheEntryEvent<K, V> {
    /** */
    private final GridCacheContinuousQueryEntry<K, V> e;

    /**
     * @param src Cache.
     * @param type Event type.
     * @param e Ignite event.
     */
    public CacheEntryEvent(IgniteCache src, EventType type, GridCacheContinuousQueryEntry<K, V> e) {
        super(src, type);

        this.e = e;
    }

    /** {@inheritDoc} */
    @Override public V getOldValue() {
        return e.getOldValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isOldValueAvailable() {
        return e.getOldValue() != null;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return e.getKey();
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return e.getValue();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> cls) {
        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheEntryEvent [evtType=" + getEventType() +
            ", key=" + getKey() +
            ", val=" + getValue() +
            ", oldVal=" + getOldValue() + ']';
    }
}
