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

package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.internal.util.CommonUtils;

/** */
public class RendezvousAffinityFunctionHelper {
    /**
     * Helper method to calculates mask.
     *
     * @param parts Number of partitions.
     * @return Mask to use in calculation when partitions count is power of 2.
     */
    public static int calculateMask(int parts) {
        return (parts & (parts - 1)) == 0 ? parts - 1 : -1;
    }

    /**
     * Helper method to calculate partition.
     *
     * @param key – Key to get partition for.
     * @param mask Mask to use in calculation when partitions count is power of 2.
     * @param parts Number of partitions.
     * @return Partition number for a given key.
     */
    public static int calculatePartition(Object key, int mask, int parts) {
        if (mask >= 0) {
            int h;

            return ((h = key.hashCode()) ^ (h >>> 16)) & mask;
        }

        return CommonUtils.safeAbs(key.hashCode() % parts);
    }
}
