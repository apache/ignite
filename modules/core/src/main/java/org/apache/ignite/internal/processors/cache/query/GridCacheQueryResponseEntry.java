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
 */
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