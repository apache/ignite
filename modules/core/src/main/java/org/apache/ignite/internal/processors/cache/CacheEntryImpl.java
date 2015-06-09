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

package org.apache.ignite.internal.processors.cache;

import javax.cache.*;
import java.io.*;

/**
 *
 */
public class CacheEntryImpl<K, V> implements Cache.Entry<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private K key;

    /** */
    private V val;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheEntryImpl() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public CacheEntryImpl(K key, V val) {
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
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if(cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (K)in.readObject();
        val = (V)in.readObject();
    }

    /** {@inheritDoc} */
    public String toString() {
        return "Entry [key=" + key + ", val=" + val + ']';
    }
}
