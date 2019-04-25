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

package org.apache.ignite.internal.processors.hadoop;

/**
 * Set of mapper utility methods.
 */
public class HadoopMapperUtils {
    /** Thread-local mapper index. */
    private static final ThreadLocal<Integer> MAP_IDX = new ThreadLocal<>();

    /**
     * @return Current mapper index.
     */
    public static int mapperIndex() {
        Integer res = MAP_IDX.get();

        return res != null ? res : -1;
    }

    /**
     * @param idx Current mapper index.
     */
    public static void mapperIndex(Integer idx) {
        MAP_IDX.set(idx);
    }

    /**
     * Clear mapper index.
     */
    public static void clearMapperIndex() {
        MAP_IDX.remove();
    }

    /**
     * Constructor.
     */
    private HadoopMapperUtils() {
        // No-op.
    }
}
