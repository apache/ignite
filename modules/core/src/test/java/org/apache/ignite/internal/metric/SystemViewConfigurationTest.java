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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.systemview.view.ConfigurationView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.IgniteKernal.CFG_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** Tests for {@link SystemView} for configuration. */
public class SystemViewConfigurationTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testConfigurationView() throws Exception {
        long expMaxSize = 10 * MB;

        String expName = "my-instance";

        String expDrName = "my-dr";

        IgniteConfiguration icfg = getConfiguration(expName)
            .setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION)
            .setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setLazyMemoryAllocation(false))
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(expDrName)
                    .setMaxSize(expMaxSize)));

        try (IgniteEx ignite = startGrid(icfg)) {
            Map<String, String> viewContent = new HashMap<>();

            ignite.context().systemView().<ConfigurationView>view(CFG_VIEW)
                .forEach(view -> viewContent.put(view.name(), view.value()));

            assertEquals(expName, viewContent.get("IgniteInstanceName"));
            assertEquals(
                "false",
                viewContent.get("DataStorageConfiguration.DefaultDataRegionConfiguration.LazyMemoryAllocation")
            );
            assertEquals(expDrName, viewContent.get("DataStorageConfiguration.DataRegionConfigurations[0].Name"));
            assertEquals(
                Long.toString(expMaxSize),
                viewContent.get("DataStorageConfiguration.DataRegionConfigurations[0].MaxSize")
            );
            assertEquals(
                CacheAtomicityMode.TRANSACTIONAL.name(),
                viewContent.get("CacheConfiguration[0].AtomicityMode")
            );
            assertTrue(viewContent.containsKey("AddressResolver"));
            assertNull(viewContent.get("AddressResolver"));
            assertEquals("[" + EVT_CONSISTENCY_VIOLATION + ']', viewContent.get("IncludeEventTypes"));
        }
    }
}
