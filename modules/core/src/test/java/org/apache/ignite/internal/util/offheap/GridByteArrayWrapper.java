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

package org.apache.ignite.internal.util.offheap;

import java.util.Arrays;

/**
 * Array wrapper for correct equals and hash code.
 */
public class GridByteArrayWrapper {
    /** Wrapped array. */
    private byte[] arr;

    /**
     * Constructor.
     *
     * @param arr Wrapped array.
     */
    public GridByteArrayWrapper(byte[] arr) {
        this.arr = arr;
    }

    /**
     * @return Wrapped byte array.
     */
    public byte[] array() {
        return arr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridByteArrayWrapper))
            return false;

        GridByteArrayWrapper that = (GridByteArrayWrapper)o;

        return Arrays.equals(arr, that.arr);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(arr);
    }
}