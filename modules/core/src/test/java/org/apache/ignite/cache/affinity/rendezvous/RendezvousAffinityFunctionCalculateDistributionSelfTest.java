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

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AbstractAffinityFunctionSelfTest;
import org.apache.ignite.cache.affinity.AffinityFunction;


/**
 * Tests for {@link RendezvousAffinityFunction}.
 */
public class RendezvousAffinityFunctionCalculateDistributionSelfTest extends AbstractAffinityFunctionSelfTest {
    /** Ignite. */
    private static Ignite ignite;

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationEnabled() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(true));

        ignite = startGrid();

        File file = new File(home() + "/work/log/ignite.log");

        assertTrue(FileUtils.readFileToString(file).contains("cacheName=ignite-sys-cache, Primary node="
            + ignite.configuration().getNodeId() + ", partitions count=100 percentage of parts count=100%"));

        assertTrue(FileUtils.readFileToString(file).contains("cacheName=ignite-atomics-sys-cache, Primary node="
            + ignite.configuration().getNodeId() + ", partitions count=1024 percentage of parts count=100%"));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationDisabled() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(false));

        ignite = startGrid();

        File file = new File(home() + "/work/log/ignite.log");

        assertFalse(FileUtils.readFileToString(file).contains("cacheName=ignite-sys-cache, Primary node="
            + ignite.configuration().getNodeId() + ", partitions count=100 percentage of parts count=100%"));

        assertFalse(FileUtils.readFileToString(file).contains("cacheName=ignite-atomics-sys-cache, Primary node="
            + ignite.configuration().getNodeId() + ", partitions count=1024 percentage of parts count=100%"));

        stopAllGrids();
    }

    /**
     * @return affinityFunction AffinityFunction
     */
    @Override protected AffinityFunction affinityFunction() {
        AffinityFunction aff = new RendezvousAffinityFunction(true, 1024);

        return aff;
    }
}