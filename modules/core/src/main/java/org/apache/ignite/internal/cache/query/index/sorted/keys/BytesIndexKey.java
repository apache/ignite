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

import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;

/** */
public class BytesIndexKey implements IndexKey {
    /** */
    protected final byte[] key;

    /** */
    public BytesIndexKey(byte[] key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.BYTES;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        byte[] arr0 = key;
        byte[] arr1 = ((BytesIndexKey)o).key;

        if (arr0 == arr1)
            return 0;

        int commonLen = Math.min(arr0.length, arr1.length);
        int unSignArr0;
        int unSignArr1;

        for (int i = 0; i < commonLen; ++i) {
            unSignArr0 = arr0[i] & 255;
            unSignArr1 = arr1[i] & 255;

            if (unSignArr0 != unSignArr1)
                return unSignArr0 > unSignArr1 ? 1 : -1;
        }

        return Integer.signum(Integer.compare(arr0.length, arr1.length));
    }
}
