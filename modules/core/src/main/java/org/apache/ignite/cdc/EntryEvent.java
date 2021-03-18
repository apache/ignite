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

package org.apache.ignite.cdc;

import java.io.Serializable;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Event for single entry change.
 * Instance presents new value of modified entry.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @see IgniteCDC
 * @see CDCConsumer
 */
@IgniteExperimental
public class EntryEvent<K, V> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Key. */
    @GridToStringInclude(sensitive = true)
    private final K key;

    /** Value. */
    @GridToStringInclude(sensitive = true)
    private final V val;

    /** {@code True} if changes made on primary node. */
    private final boolean primary;

    /** Partition. */
    private final int part;

    /** Order of the entry change. */
    private final EntryEventOrder ord;

    /** Event type. */
    private final EntryEventType op;

    /** Cache id. */
    private final int cacheId;

    /** Entry expire time. */
    private final long expireTime;

    /**
     * @param key Key.
     * @param val Value.
     * @param primary {@code True} if changes made on primary node.
     * @param part Partition.
     * @param ord Order of the entry change.
     * @param op Event type.
     * @param cacheId Cache id.
     * @param expireTime Entry expire time.
     */
    public EntryEvent(K key, V val, boolean primary, int part,
        EntryEventOrder ord, EntryEventType op, int cacheId, long expireTime) {
        this.key = key;
        this.val = val;
        this.primary = primary;
        this.part = part;
        this.ord = ord;
        this.op = op;
        this.cacheId = cacheId;
        this.expireTime = expireTime;
    }

    /**
     * @return Key for the changed entry.
     */
    public K key() {
        return key;
    }

    /**
     * @return Value for the changed entry.
     */
    public V value() {
        return val;
    }

    /**
     * @return {@code True} if event fired on primary node for partition containing this entry.
     * @see <a href="https://ignite.apache.org/docs/latest/configuring-caches/configuring-backups#configuring-partition-backups">Configuring partition backups.</a>
     */
    public boolean primary() {
        return primary;
    }

    /**
     * @return Partition number.
     */
    public int partition() {
        return part;
    }

    /**
     * @return Order of the update operation.
     */
    public EntryEventOrder order() {
        return ord;
    }

    /**
     * @return Operation type.
     */
    public EntryEventType operation() {
        return op;
    }

    /**
     * @return Cache ID.
     * @see org.apache.ignite.internal.util.typedef.internal.CU#cacheId(String)
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Expire time.
     */
    long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EntryEvent.class, this);
    }
}
