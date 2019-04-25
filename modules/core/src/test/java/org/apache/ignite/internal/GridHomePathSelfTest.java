/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;

/**
 *
 */
public class GridHomePathSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost(getTestResources().getLocalHost());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHomeOverride() throws Exception {
        try {
            startGrid(0);

            // Test home override.
            IgniteConfiguration c = getConfiguration(getTestIgniteInstanceName(1));

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
            IgniteConfiguration c1 = getConfiguration(getTestIgniteInstanceName(1));

            c1.setIgniteHome(System.getProperty(IGNITE_HOME));

            G.start(c1);
        }
        finally {
            stopAllGrids();
        }
    }
}
