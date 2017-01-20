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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * New behavior for node ordering. Sort firstly by region number, and secondly by old order.
 */
public class RegionNodeComparator implements Comparator<TcpDiscoveryNode> {
    private Long getRegionId(TcpDiscoveryNode node) {
        Object id = node.getAttributes().get("CLUSTER_REGION_ID");
        final Long ans;
        if (id==null) ans = null;
        else if (id instanceof Number) ans = ((Number) id).longValue();
        else if (id instanceof String) ans = Long.valueOf((String)id);
        else ans = null;
        return ans;
    }

    /** {@inheritDoc} */
    @Override
    public int compare(TcpDiscoveryNode nodeFirst, TcpDiscoveryNode nodeSecond) {
        Long first = getRegionId(nodeFirst);
        Long second = getRegionId(nodeSecond);

        final int ans;

        if (first==second)
            ans = nodeFirst.compareTo(nodeSecond);
        else {
            if (first==null)
                ans = -1;
            else if (second==null)
                ans = 1;
            else ans = (first>second)?1:-1;
        }

        return ans;
    }
}
