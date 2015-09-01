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

package org.apache.ignite.internal.processors.rest;

import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Rest processor test.
 */
public class RestProcessorMultiStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     * Test that multiple nodes can start with JETTY enabled.
     *
     * @throws Exception If failed.
     */
    public void testMultiStart() throws Exception {
        try {
            for (int i = 0; i < GRID_CNT; i++)
                startGrid(i);

            stopGrid(0);
        }
        finally {
            stopAllGrids();
        }
    }
}