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

package org.apache.ignite.cache.version;

import javax.cache.*;
import java.util.*;

/**
 * Cache entry along with version information.
 */
public interface VersionedEntry<K, V> extends Cache.Entry<K, V> {
    /**
     * Versions comparator.
     */
    public static final Comparator<VersionedEntry> VERSIONS_COMPARATOR = new Comparator<VersionedEntry>() {
        @Override public int compare(VersionedEntry o1, VersionedEntry o2) {
            int res = Integer.compare(o1.topologyVersion(), o2.topologyVersion());

            if (res != 0)
                return res;

            res = Long.compare(o1.order(), o2.order());

            if (res != 0)
                return res;

            return Integer.compare(o1.nodeOrder(), o2.nodeOrder());
        }
    };

    /**
     * Gets entry's topology version.
     *
     * @return Topology version plus number of seconds from the start time of the first grid node.
     */
    public int topologyVersion();

    /**
     * Gets entry's order.
     *
     * @return Version order.
     */
    public long order();

    /**
     * Gets entry's node order.
     *
     * @return Node order on which this version was assigned.
     */
    public int nodeOrder();

    /**
     * Gets entry's global time.
     *
     * @return Adjusted time.
     */
    public long globalTime();
}
