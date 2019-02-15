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

package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Plain versioned entry.
 */
public class GridCachePlainVersionedEntry<K, V> implements GridCacheVersionedEntryEx<K, V> {
    /** Key. */
    @GridToStringInclude
    protected K key;

    /** Value. */
    @GridToStringInclude
    protected V val;

    /** TTL. */
    private final long ttl;

    /** Expire time. */
    private final long expireTime;

    /** Version. */
    private final GridCacheVersion ver;

    /** Start version flag. */
    private final boolean isStartVer;

    /**
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     */
    public GridCachePlainVersionedEntry(K key, @Nullable V val, long ttl, long expireTime, GridCacheVersion ver) {
        this(key, val, ttl, expireTime, ver, false);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param isStartVer Start version flag.
     */
    public GridCachePlainVersionedEntry(K key, V val, long ttl, long expireTime, GridCacheVersion ver,
        boolean isStartVer) {
        this.key = key;
        this.val = val;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
        this.isStartVer = isStartVer;
    }

    /** {@inheritDoc} */
    @Override public K key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V value(CacheObjectValueContext ctx) {
        return val;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public byte dataCenterId() {
        return ver.dataCenterId();
    }

    /** {@inheritDoc} */
    @Override public int topologyVersion() {
        return ver.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return ver.order();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean isStartVersion() {
        return isStartVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePlainVersionedEntry.class, this);
    }
}
