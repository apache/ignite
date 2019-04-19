/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.util.Arrays;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Test object for placing into grid in this test */
class IndexedObject {
    /** I value. */
    @QuerySqlField(index = true)
    int iVal;

    /** J value = I value. */
    @QuerySqlField(index = true)
    int jVal;

    /** Data filled with recognizable pattern */
    private byte[] data;

    /**
     * @param iVal Integer value.
     */
    IndexedObject(int iVal) {
        this.iVal = iVal;
        this.jVal = iVal;
        int sz = 1024;
        data = new byte[sz];
        for (int i = 0; i < sz; i++)
            data[i] = (byte)('A' + (i % 10));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IndexedObject obj = (IndexedObject)o;

        if (iVal != obj.iVal)
            return false;
        return Arrays.equals(data, obj.data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = iVal;
        res = 31 * res + Arrays.hashCode(data);
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexedObject.class, this);
    }

    /** @return bytes data */
    public byte[] getData() {
        return data;
    }
}
