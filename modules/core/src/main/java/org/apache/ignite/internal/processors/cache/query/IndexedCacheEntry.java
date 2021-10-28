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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.lang.IgniteBiTuple;

/** Represents cache key-value pair and indexed columns. */
public class IndexedCacheEntry<K, V> extends IgniteBiTuple<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Indexed keys. */
    private IndexKey[] idxKeys;

    /** Count of indexed keys. */
    private int keysSize;

    /** Mask for indexed keys order. */
    private int orderMask;

    /** */
    public IndexedCacheEntry() {
        // No-op.
    }

    /** */
    public IndexedCacheEntry(K key, V val, IndexKey[] keys, int keysSize, int orderMask) {
        super(key, val);

        this.keysSize = keysSize;
        idxKeys = keys;
        this.orderMask = orderMask;
    }

    /** */
    public IndexKey key(int idx) {
        return idxKeys[idx];
    }

    /** */
    public int orderMask() {
        return orderMask;
    }

    /** */
    public int keysSize() {
        return keysSize;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(orderMask);
        out.writeInt(keysSize);

        for (int i = 0; i < keysSize; i++)
            out.writeObject(idxKeys[i]);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        orderMask = in.readInt();
        keysSize = in.readInt();

        idxKeys = new IndexKey[keysSize];

        for (int i = 0; i < keysSize; i++)
            idxKeys[i] = (IndexKey)in.readObject();
    }
}

