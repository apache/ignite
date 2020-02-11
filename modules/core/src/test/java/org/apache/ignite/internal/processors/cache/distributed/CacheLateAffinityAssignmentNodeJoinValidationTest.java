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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheLateAffinityAssignmentNodeJoinValidationTest extends GridCommonAbstractTest {
    /** */
    private boolean lateAff;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLateAffinityAssignment(lateAff);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinValidation1() throws Exception {
        checkNodeJoinValidation(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinValidation2() throws Exception {
        checkNodeJoinValidation(true);
    }

    /**
     * @param firstEnabled Flag value for first started node.
     * @throws Exception If failed.
     */
    private void checkNodeJoinValidation(boolean firstEnabled) throws Exception {
        // LateAffinity should be always enabled, setLateAffinityAssignment should be ignored.
        lateAff = firstEnabled;

        Ignite ignite = startGrid(0);

        assertFalse(ignite.configuration().isClientMode());

        lateAff = !firstEnabled;

        startGrid(1);

        startClientGrid(2);

        assertEquals(3, ignite.cluster().nodes().size());
    }
}
