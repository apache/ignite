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