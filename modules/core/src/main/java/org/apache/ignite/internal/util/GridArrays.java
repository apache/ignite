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

package org.apache.ignite.internal.util;

import java.util.Arrays;

/**
 *
 */
public final class GridArrays {
    /**
     * Constructor.
     */
    private GridArrays() {
        // No-op.
    }

    /**
     * Set element to the array at the given index. Grows the array if needed.
     *
     * @param arr Array.
     * @param idx Index.
     * @param o Object.
     * @return The given or grown array.
     */
    public static <T> T[] set(T[] arr, int idx, T o) {
        int len = arr.length;

        if (idx >= len) {
            len += len >>> 1; // len *= 1.5
            len = Math.max(len, idx + 1);
            arr = Arrays.copyOf(arr, len);
        }

        arr[idx] = o;

        return arr;
    }

    /**
     * Nullify array elements from the given index until the first {@code null} element
     * (assuming that after the first {@code null} tail is already cleared).
     *
     * @param arr Array.
     * @param fromIdx From index (including).
     */
    public static void clearTail(Object[] arr, int fromIdx) {
        while (fromIdx < arr.length && arr[fromIdx] != null)
            arr[fromIdx++] = null;
    }
}
