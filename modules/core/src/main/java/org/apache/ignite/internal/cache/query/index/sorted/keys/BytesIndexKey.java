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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;

/** */
public class BytesIndexKey implements IndexKey {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected byte[] key;

    /** */
    public BytesIndexKey(byte[] key) {
        this.key = key;
    }

    /** */
    public BytesIndexKey() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public int type() {
        return IndexKeyTypes.BYTES;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return BytesCompareUtils.compareNotNullUnsigned(key, ((BytesIndexKey)o).key);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(key.length);
        out.write(key, 0, key.length);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        key = new byte[len];

        in.read(key);
    }
}
