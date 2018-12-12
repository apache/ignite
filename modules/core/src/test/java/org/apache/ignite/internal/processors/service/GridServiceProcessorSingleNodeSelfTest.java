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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Single node services test.
 */
@RunWith(JUnit4.class)
public class GridServiceProcessorSingleNodeSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeSingletonNotDeployedProxy() throws Exception {
        String name = "testNodeSingletonNotDeployedProxy";

        Ignite ignite = randomGrid();

        // Deploy only on remote nodes.
        ignite.services(ignite.cluster().forRemotes()).deployNodeSingleton(name, new CounterServiceImpl());

        info("Deployed service: " + name);

        // Get local proxy.
        CounterService svc = ignite.services().serviceProxy(name, CounterService.class, false);

        try {
            svc.increment();

            fail("Should never reach here.");
        }
        catch (IgniteException e) {
            info("Got expected exception: " + e.getMessage());
        }
    }
}
