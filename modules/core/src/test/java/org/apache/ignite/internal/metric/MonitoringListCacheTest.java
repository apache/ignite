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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.metric.list.view.CacheView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MonitoringListCacheTest extends GridCommonAbstractTest {
    @Test
    /** */
    public void testCachesList() throws Exception {
        IgniteEx g = startGrid();

        Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

        for (String name : cacheNames)
            g.createCache(name);

        MonitoringList<String, CacheView> caches = g.context().metric().list("caches");

        for (CacheView row : caches) {
            cacheNames.remove(row.cacheName());
        }

        assertTrue(cacheNames.isEmpty());
    }

    @Test
    /** */
    public void testCacheGroupsList() throws Exception {
        IgniteEx g = startGrid();

        g.createCache(new CacheConfiguration<>("cache-1").setGroupName("group-1"));
        g.createCache(new CacheConfiguration<>("cache-2").setGroupName("group-2"));

        MonitoringList<MonitoringRow<String>> groups = g.context().metric().list("cacheGroups");


    }
}
