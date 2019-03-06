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

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Lazy plain versioned entry.
 */
public class GridCacheLazyPlainVersionedEntry<K, V> extends GridCachePlainVersionedEntry<K, V> {
    /** Cache context. */
    protected GridCacheContext cctx;

    /** Key cache object. */
    private KeyCacheObject keyObj;

    /** Cache object value. */
    private CacheObject valObj;

    /** Keep binary flag. */
    private boolean keepBinary;

    /**
     * @param cctx Context.
     * @param keyObj Key.
     * @param valObj Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param isStartVer Start version flag.
     * @param keepBinary Keep binary flag.
     */
    public GridCacheLazyPlainVersionedEntry(GridCacheContext cctx,
        KeyCacheObject keyObj,
        CacheObject valObj,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        boolean isStartVer,
        boolean keepBinary) {
        super(null, null, ttl, expireTime, ver, isStartVer);

        this.cctx = cctx;
        this.keyObj = keyObj;
        this.valObj = valObj;
        this.keepBinary = keepBinary;
    }

    public GridCacheLazyPlainVersionedEntry(GridCacheContext cctx,
        KeyCacheObject keyObj,
        CacheObject valObj,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        boolean keepBinary) {
        super(null, null, ttl, expireTime, ver);
        this.cctx = cctx;
        this.keepBinary = keepBinary;
        this.keyObj = keyObj;
        this.valObj = valObj;
    }

    /** {@inheritDoc} */
    @Override public K key() {
        if (key == null)
            key = (K)cctx.unwrapBinaryIfNeeded(keyObj, keepBinary);

        return key;
    }

    /** {@inheritDoc} */
    @Override public V value(CacheObjectValueContext ctx) {
        return value(keepBinary);
    }

    /**
     * Returns the value stored in the cache when this entry was created.
     *
     * @param keepBinary Flag to keep binary if needed.
     * @return the value corresponding to this entry
     */
    public V value(boolean keepBinary) {
        if (val == null)
            val = (V)cctx.unwrapBinaryIfNeeded(valObj, keepBinary, true);

        return val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLazyPlainVersionedEntry.class, this,
            "super", super.toString(), "key", key(), "val", value(keepBinary));
    }
}
