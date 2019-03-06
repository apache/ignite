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

package org.apache.ignite.internal.processors.cache.query.continuous;

import javax.cache.Cache;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Continuous query event.
 */
class CacheContinuousQueryEvent<K, V> extends CacheQueryEntryEvent<K, V> {
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

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return e.partition();
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return (K)cctx.cacheObjectContext().unwrapBinaryIfNeeded(e.key(), e.isKeepBinary(), false);
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(e.value(), e.isKeepBinary(), false);
    }

    /** {@inheritDoc} */
    @Override public V getOldValue() {
        return (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(e.oldValue(), e.isKeepBinary(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean isOldValueAvailable() {
        return e.oldValue() != null;
    }

    /** {@inheritDoc} */
    @Override public long getPartitionUpdateCounter() {
        return e.updateCounter();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryEvent.class, this,
            "evtType", getEventType(), false,
            "key", getKey(), true,
            "newVal", getValue(), true,
            "oldVal", getOldValue(), true,
            "partCntr", getPartitionUpdateCounter(), false);
    }
}
