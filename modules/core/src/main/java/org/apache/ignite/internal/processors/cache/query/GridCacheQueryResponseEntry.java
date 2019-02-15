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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

/**
 * Class to store query results returned by remote nodes. It's required to fully
 * control serialization process. Local entries can be returned to user as is.
 * <p>
 * @deprecated Should be removed in Apache Ignite 3.0.
 */
@Deprecated
public class GridCacheQueryResponseEntry<K, V> implements Map.Entry<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private K key;

    /** */
    @GridToStringInclude
    private V val;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueryResponseEntry() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public GridCacheQueryResponseEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /** @return Key. */
    @Override public K getKey() {
        return key;
    }

    /**
     * @return Value.
     */
    @Override public V getValue() {
        return val;
    }

    /**
     * @param val Value
     */
    @Override public V setValue(V val) {
        this.val = val;

        return val;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (K)in.readObject();
        val = (V)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueryResponseEntry entry = (GridCacheQueryResponseEntry)o;

        return key.equals(entry.key);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "[" + key + "=" + val + "]";
    }
}
