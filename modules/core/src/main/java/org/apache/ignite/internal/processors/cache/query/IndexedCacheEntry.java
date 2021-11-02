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
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.lang.IgniteBiTuple;

/** Represents cache key-value pair and indexed columns. */
public class IndexedCacheEntry<K, V> extends IgniteBiTuple<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Indexed keys. */
    private IndexKey[] idxKeys;

    /** Index row. */
    private IndexRow idxRow;

    /** Count of indexed keys used in a query. */
    private int keysSize;

    /** */
    public IndexedCacheEntry() {
        // No-op.
    }

    /** */
    public IndexedCacheEntry(K key, V val, IndexRow row, int keysSize) {
        super(key, val);

        idxRow = row;
        this.keysSize = keysSize;
    }

    /** */
    public IndexKey key(int idx) {
        if (idxKeys == null)
            return idxRow.key(idx);

        return idxKeys[idx];
    }

    /** */
    public int keysSize() {
        return keysSize;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(keysSize);

        for (int i = 0; i < keysSize; i++)
            out.writeObject(idxRow.key(i));
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        keysSize = in.readInt();

        idxKeys = new IndexKey[keysSize];

        for (int i = 0; i < keysSize; i++)
            idxKeys[i] = (IndexKey)in.readObject();
    }
}
