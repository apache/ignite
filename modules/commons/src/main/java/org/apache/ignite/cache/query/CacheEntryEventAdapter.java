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

package org.apache.ignite.cache.query;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

import static javax.cache.event.EventType.EXPIRED;
import static javax.cache.event.EventType.REMOVED;

/** */
public abstract class CacheEntryEventAdapter<K, V> extends CacheEntryEvent<K, V> {
    /** */
    protected CacheEntryEventAdapter(Cache src, EventType evtType) {
        super(src, evtType);
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        EventType evtType = getEventType();

        return (evtType == EXPIRED || evtType == REMOVED) ? getOldValue() : getNewValue();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** */
    protected abstract V getNewValue();
}
