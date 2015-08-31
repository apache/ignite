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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Atomic cache version comparator.
 */
public class GridCacheAtomicVersionComparator {
    /**
     * Compares two cache versions.
     *
     * @param one First version.
     * @param other Second version.
     * @param ignoreTime {@code True} if global time should be ignored.
     * @return Comparison value.
     */
    public int compare(GridCacheVersion one, GridCacheVersion other, boolean ignoreTime) {
        int topVer = one.topologyVersion();
        int otherTopVer = other.topologyVersion();

        if (topVer == otherTopVer) {
            long globalTime = one.globalTime();
            long otherGlobalTime = other.globalTime();

            if (globalTime == otherGlobalTime || ignoreTime) {
                long locOrder = one.order();
                long otherLocOrder = other.order();

                if (locOrder == otherLocOrder) {
                    int nodeOrder = one.nodeOrder();
                    int otherNodeOrder = other.nodeOrder();

                    return nodeOrder == otherNodeOrder ? 0 : nodeOrder < otherNodeOrder ? -1 : 1;
                }
                else
                    return locOrder > otherLocOrder ? 1 : -1;
            }
            else
                return globalTime > otherGlobalTime ? 1 : -1;
        }
        else
            return topVer > otherTopVer ? 1 : -1;
    }
}