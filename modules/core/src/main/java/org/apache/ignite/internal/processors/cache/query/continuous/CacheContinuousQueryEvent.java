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

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Continuous query event.
 */
class CacheContinuousQueryEvent<K, V> extends CacheEntryEvent<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridCacheContext cctx;

    /** Entry. */
    @GridToStringExclude
    private final CacheContinuousQueryEntry e;

    /**
     * @param src Source cache.
     * @param cctx Cache context.
     * @param e Entry.
     */
    CacheContinuousQueryEvent(Cache src, GridCacheContext cctx, CacheContinuousQueryEntry e) {
        super(src, e.eventType());

        this.cctx = cctx;
        this.e = e;
    }

    /**
     * @return Entry.
     */
    CacheContinuousQueryEntry entry() {
        return e;
    }

    /** {@inheritDoc} */
    @Override
    public K getKey() {
        return e.key().value(cctx.cacheObjectContext(), false);
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return CU.value(e.value(), cctx, false);
    }

    /** {@inheritDoc} */
    @Override
    public V getOldValue() {
        return CU.value(e.oldValue(), cctx, false);
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
        return S.toString(CacheContinuousQueryEvent.class, this,
            "evtType", getEventType(),
            "key", getKey(),
            "newVal", getValue(),
            "oldVal", getOldValue());
    }
}