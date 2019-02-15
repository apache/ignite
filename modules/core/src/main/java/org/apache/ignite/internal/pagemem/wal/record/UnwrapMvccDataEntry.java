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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Data Entry for automatic unwrapping key and value from Mvcc Data Entry
 */
public class UnwrapMvccDataEntry extends MvccDataEntry implements UnwrappedDataEntry {
    /** Cache object value context. Context is used for unwrapping objects. */
    private final CacheObjectValueContext cacheObjValCtx;

    /** Keep binary. This flag disables converting of non primitive types (BinaryObjects). */
    private boolean keepBinary;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param val Value or null for delete operation.
     * @param op Operation.
     * @param nearXidVer Near transaction version.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param partId Partition ID.
     * @param partCnt Partition counter.
     * @param mvccVer Mvcc version.
     * @param cacheObjValCtx cache object value context for unwrapping objects.
     * @param keepBinary disable unwrapping for non primitive objects, Binary Objects would be returned instead.
     */
    public UnwrapMvccDataEntry(
        final int cacheId,
        final KeyCacheObject key,
        final CacheObject val,
        final GridCacheOperation op,
        final GridCacheVersion nearXidVer,
        final GridCacheVersion writeVer,
        final long expireTime,
        final int partId,
        final long partCnt,
        MvccVersion mvccVer,
        final CacheObjectValueContext cacheObjValCtx,
        final boolean keepBinary) {
        super(cacheId, key, val, op, nearXidVer, writeVer, expireTime, partId, partCnt, mvccVer);

        this.cacheObjValCtx = cacheObjValCtx;
        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override public Object unwrappedKey() {
        try {
            if (keepBinary && key instanceof BinaryObject)
                return key;

            Object unwrapped = key.value(cacheObjValCtx, false);

            if (unwrapped instanceof BinaryObject) {
                if (keepBinary)
                    return unwrapped;
                unwrapped = ((BinaryObject)unwrapped).deserialize();
            }

            return unwrapped;
        }
        catch (Exception e) {
            cacheObjValCtx.kernalContext().log(UnwrapMvccDataEntry.class)
                .error("Unable to convert key [" + key + "]", e);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public Object unwrappedValue() {
        try {
            if (val == null)
                return null;

            if (keepBinary && val instanceof BinaryObject)
                return val;

            return val.value(cacheObjValCtx, false);
        }
        catch (Exception e) {
            cacheObjValCtx.kernalContext().log(UnwrapMvccDataEntry.class)
                .error("Unable to convert value [" + value() + "]", e);
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + "[k = " + unwrappedKey() + ", v = [ "
            + unwrappedValue()
            + "], super = ["
            + super.toString() + "]]";
    }
}
