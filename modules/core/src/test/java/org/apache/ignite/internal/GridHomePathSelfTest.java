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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;

/**
 *
 */
public class GridHomePathSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(getTestResources().getLocalHost());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testHomeOverride() throws Exception {
        try {
            startGrid(0);

            // Test home override.
            IgniteConfiguration c = getConfiguration(getTestGridName(1));

            c.setIgniteHome("/new/path");

            try {
                G.start(c);

                assert false : "Exception should have been thrown.";
            }
            catch (Exception e) {
                if (X.hasCause(e, IgniteException.class))
                    info("Caught expected exception: " + e);
                else
                    throw e;
            }

            // Test no override.
            IgniteConfiguration c1 = getConfiguration(getTestGridName(1));

            c1.setIgniteHome(System.getProperty(IGNITE_HOME));

            G.start(c1);
        }
        finally {
            stopAllGrids();
        }
    }
}