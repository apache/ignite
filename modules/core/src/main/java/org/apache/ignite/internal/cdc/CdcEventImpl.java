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
import org.apache.ignite.internal.util.typedef.internal.S;

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

    /** Key. */
    private final Object key;

    /** Value. */
    private final Object val;

    /** {@code True} if changes made on primary node. */
    private final boolean primary;

    /** Partition. */
    private final int part;

    /** Order of the entry change. */
    private final CacheEntryVersion ord;

    /** Cache id. */
    private final int cacheId;

    /** Expire time. */
    private final long expireTime;

    /**
     * @param key Key.
     * @param val Value.
     * @param primary {@code True} if changes made on primary node.
     * @param part Partition.
     * @param ord Order of the entry change.
     * @param cacheId Cache id.
     * @param expireTime Expire time.
     */
    public CdcEventImpl(
        Object key,
        Object val,
        boolean primary,
        int part,
        CacheEntryVersion ord,
        int cacheId,
        long expireTime
    ) {
        this.key = key;
        this.val = val;
        this.primary = primary;
        this.part = part;
        this.ord = ord;
        this.cacheId = cacheId;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        return primary;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public CacheEntryVersion version() {
        return ord;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CdcEventImpl.class, this);
    }
}
