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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.GET_ENTRY_INVALID_VER_AFTER_GET;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry.GET_ENTRY_INVALID_VER_UPDATED;

/**
 *
 */
public class CacheEntryImplEx<K, V> extends CacheEntryImpl<K, V> implements CacheEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheEntryImplEx() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value (always null).
     * @param ver Version.
     */
    public CacheEntryImplEx(K key, V val, GridCacheVersion ver) {
        super(key, val);

        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        if (ver == GET_ENTRY_INVALID_VER_AFTER_GET) {
            throw new IgniteException("Impossible to get entry version after " +
                "get() inside OPTIMISTIC REPEATABLE_READ transaction. Use only getEntry() or getEntries() inside " +
                "OPTIMISTIC REPEATABLE_READ transaction to solve this problem.");
        }
        else if (ver == GET_ENTRY_INVALID_VER_UPDATED)
            throw new IgniteException("Impossible to get version for entry updated in transaction.");

        return ver;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        ver = (GridCacheVersion)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String res = "CacheEntry [key=" + getKey() +
            ", val=" + getValue();

        if (ver != null && ver != GET_ENTRY_INVALID_VER_AFTER_GET && ver != GET_ENTRY_INVALID_VER_UPDATED) {
            res += ", topVer=" + ver.topologyVersion() +
                ", nodeOrder=" + ver.nodeOrder() +
                ", order=" + ver.order();
        }
        else
            res += ", ver=n/a";

        return res + ']';
    }
}
