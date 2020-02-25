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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
final class DistributedMetaStorageHistoryItem extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final DistributedMetaStorageHistoryItem[] EMPTY_ARRAY = {};

    /** */
    @GridToStringInclude
    private String[] keys;

    /** */
    @GridToStringInclude
    private byte[][] valBytesArr;

    /** */
    private transient long longHash;

    /** Default constructor for deserialization. */
    public DistributedMetaStorageHistoryItem() {
        // No-op.
    }

    /** */
    public DistributedMetaStorageHistoryItem(String key, byte[] valBytes) {
        keys = new String[] {key};
        valBytesArr = new byte[][] {valBytes};
    }

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public DistributedMetaStorageHistoryItem(String[] keys, byte[][] valBytesArr) {
        assert keys.length > 0;
        assert keys.length == valBytesArr.length;

        this.keys = keys;
        this.valBytesArr = valBytesArr;
    }

    /** */
    public long estimateSize() {
        int len = keys.length;

        // 8L = 2 int sizes. Plus all int sizes of strings and byte arrays.
        long size = 8L + 8L * len;

        // String encoding is ignored to make estimation faster. 2 "size" values added as well.
        for (int i = 0; i < len; i++)
            size += keys[i].length() * 2 + (valBytesArr[i] == null ? 0 : valBytesArr[i].length);

        return size;
    }

    /**
     * Array of keys modified in this update.
     */
    public String[] keys() {
        //noinspection AssignmentOrReturnOfFieldWithMutableType
        return keys;
    }

    /**
     * Array of serialized values corresponded to {@link #keys()} in the same order.
     */
    public byte[][] valuesBytesArray() {
        //noinspection AssignmentOrReturnOfFieldWithMutableType
        return valBytesArr;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(keys.length);

        for (int i = 0; i < keys.length; i++) {
            U.writeString(out, keys[i]);

            U.writeByteArray(out, valBytesArr[i]);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        int len = in.readInt();

        keys = new String[len];
        valBytesArr = new byte[len][];

        for (int i = 0; i < len; i++) {
            keys[i] = U.readString(in);
            valBytesArr[i] = U.readByteArray(in);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DistributedMetaStorageHistoryItem item = (DistributedMetaStorageHistoryItem)o;

        return Arrays.equals(keys, item.keys) && Arrays.deepEquals(valBytesArr, item.valBytesArr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Long.hashCode(longHash());
    }

    /** Long hash. */
    public long longHash() {
        long hash = longHash;

        if (hash == 0L) {
            hash = 1L;

            for (String key : keys)
                hash = hash * 31L + key.hashCode();

            for (byte[] valBytes : valBytesArr) {
                if (valBytes == null)
                    hash *= 31L;
                else
                    for (byte b : valBytes)
                        hash = hash * 31L + b;
            }

            if (hash == 0L)
                hash = 1L; // Avoid rehashing.

            longHash = hash;
        }

        return hash;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageHistoryItem.class, this);
    }
}
