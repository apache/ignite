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
     * @return Comparison value.
     */
    public int compare(GridCacheVersion one, GridCacheVersion other) {
        int topVer = one.topologyVersion();
        int otherTopVer = other.topologyVersion();

        if (topVer == otherTopVer) {
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
            return topVer > otherTopVer ? 1 : -1;
    }
}