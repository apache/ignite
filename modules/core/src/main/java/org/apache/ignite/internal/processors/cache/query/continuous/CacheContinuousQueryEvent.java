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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.*;
import javax.cache.event.*;

/**
 * Continuous query event.
 */
class CacheContinuousQueryEvent<K, V> extends CacheEntryEvent<K, V> {
    /** Entry. */
    @GridToStringExclude
    private final CacheContinuousQueryEntry<K, V> e;

    /**
     * @param source Source cache.
     * @param eventType Event type.
     * @param e Entry.
     */
    CacheContinuousQueryEvent(Cache source, EventType eventType, CacheContinuousQueryEntry<K, V> e) {
        super(source, eventType);

        assert e != null;

        this.e = e;
    }

    /**
     * @return Entry.
     */
    CacheContinuousQueryEntry<K, V> entry() {
        return e;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return e.key();
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return e.value();
    }

    /** {@inheritDoc} */
    @Override public V getOldValue() {
        return e.oldValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isOldValueAvailable() {
        return e.oldValue() != null;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> cls) {
        if(cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryEvent.class, this, "key", e.key(), "newVal", e.value(), "oldVal",
            e.oldValue(), "cacheName", e.cacheName());
    }
}
