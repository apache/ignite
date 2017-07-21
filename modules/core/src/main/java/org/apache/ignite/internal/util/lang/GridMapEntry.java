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

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Simple {@link java.util.Map.Entry} implementation.
 */
public class GridMapEntry<K, V> implements Map.Entry<K, V>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private K key;

    /** */
    @GridToStringInclude
    private V val;

    /**
     * @param key Key.
     * @param val Value.
     */
    public GridMapEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public V setValue(V val) {
        V old = this.val;

        this.val = val;

        return old;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridMapEntry e = (GridMapEntry)o;

        return F.eq(key, e.key) && F.eq(val, e.val);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (key != null ? key.hashCode() : 0) + (val != null ? val.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMapEntry.class, this);
    }
}