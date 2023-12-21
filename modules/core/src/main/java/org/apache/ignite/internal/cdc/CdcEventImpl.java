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

package org.apache.ignite.internal.cdc;

import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Event of single entry change.
 * Instance presents new value of modified entry.
 *
 * @see CdcMain
 * @see CdcConsumer
 */
public class CdcEventImpl implements CdcEvent {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Entry. */
    private final DataEntry entry;

    /**
     * @param entry Entry.
     */
    public CdcEventImpl(DataEntry entry) {
        this.entry = entry;
    }

    /** {@inheritDoc} */
    @Override public Object unwrappedKey() {
        return ((UnwrapDataEntry)(entry)).unwrappedKey();
    }

    /** {@inheritDoc} */
    @Override public Object unwrappedValue() {
        return ((UnwrapDataEntry)(entry)).unwrappedValue();
    }

    /** {@inheritDoc} */
    @Override public Object unwrappedPreviousStateMetadata() {
        return ((UnwrapDataEntry)(entry)).unwrappedPreviousStateMetadata();
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return entry.key();
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        return entry.value();
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject previousStateMetadata() {
        return entry.previousStateMetadata();
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        return (entry.flags() & DataEntry.PRIMARY_FLAG) != 0;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return entry.partitionId();
    }

    /** {@inheritDoc} */
    @Override public CacheEntryVersion version() {
        return entry.writeVersion();
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return entry.cacheId();
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return entry.expireTime();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CdcEventImpl.class, this);
    }
}
