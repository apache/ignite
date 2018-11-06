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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheRebalanceConfigValidationTest extends GridCommonAbstractTest {
    /** Rebalance pool size. */
    private int rebalancePoolSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setRebalanceThreadPoolSize(rebalancePoolSize);

        return cfg;
    }

    /**
     * Checks that node is not allowed to join to cluster if has different value of {@link IgniteConfiguration#rebalanceThreadPoolSize}.
     *
     * @throws Exception If failed.
     */
    public void testParameterConsistency() throws Exception {
        rebalancePoolSize = 2;

        startGrid(0);

        rebalancePoolSize = 1;

        GridTestUtils.assertThrows(log, () -> startGrid(1), IgniteCheckedException.class, "Rebalance configuration mismatch");
    }
}
