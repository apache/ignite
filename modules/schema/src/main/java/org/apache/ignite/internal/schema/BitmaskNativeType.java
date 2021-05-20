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

package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.tostring.S;

/**
 * A fixed-sized type representing a bitmask of <code>n</code> bits. The actual size of a bitmask will round up
 * to the smallest number of bytes required to store <code>n</code> bits.
 */
public class BitmaskNativeType extends NativeType {
    /** */
    private final int bits;

    /**
     * Creates a bitmask type of size <code>bits</code>. In row will round up to the closest full byte.
     *
     * @param bits The number of bits in the bitmask.
     */
    protected BitmaskNativeType(int bits) {
        super(NativeTypeSpec.BITMASK, (bits + 7) / 8);

        this.bits = bits;
    }

    /**
     * @return Maximum number of bits to be stored in the bitmask.
     */
    public int bits() {
        return bits;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        BitmaskNativeType that = (BitmaskNativeType)o;

        return bits == that.bits;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return bits;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        int res = super.compareTo(o);

        if (res == 0) {
            // The passed in object is also a bitmask, compare the number of bits.
            BitmaskNativeType that = (BitmaskNativeType)o;

            return Integer.compare(bits, that.bits);
        }
        else
            return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BitmaskNativeType.class.getSimpleName(), "bits", bits, "typeSpec", spec(), "len", sizeInBytes());
    }
}
