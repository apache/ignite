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

package org.apache.ignite.internal.binary;

import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/**
 * Tests a check of binary configuration consistency.
 */
public class BinaryConfigurationConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    private BinaryConfiguration binaryCfg;

    /** */
    private boolean isClient;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(isClient);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setBinaryConfiguration(binaryCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        isClient = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSkipCheckConsistencyFlagEnabled() throws Exception {
        String backup = System.setProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, "true");

        try {
            // Wrong usage of Ignite (done only in test purposes).
            binaryCfg = null;

            startGrid(0);

            binaryCfg = new BinaryConfiguration();

            startGrid(1);

            isClient = true;
            binaryCfg = customConfig(true);

            startGrid(2);
        }
        finally {
            if (backup != null)
                System.setProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, backup);
            else
                System.clearProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPositiveNullConfig() throws Exception {
        binaryCfg = null;

        startGrids(2);

        isClient = true;

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPositiveEmptyConfig() throws Exception {
        binaryCfg = new BinaryConfiguration();

        startGrids(2);

        isClient = true;

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPositiveCustomConfig() throws Exception {
        binaryCfg = customConfig(false);

        startGrids(2);

        isClient = true;

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNegativeNullEmptyConfigs() throws Exception {
        checkNegative(null, new BinaryConfiguration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNegativeEmptyNullConfigs() throws Exception {
        checkNegative(new BinaryConfiguration(), null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNegativeEmptyCustomConfigs() throws Exception {
        checkNegative(new BinaryConfiguration(), customConfig(false));
    }


    /**
     * @throws Exception If failed.
     */
    public void testNegativeCustomNullConfigs() throws Exception {
        checkNegative(customConfig(false), null);
    }

    /**
     * @param bCfg1 BinaryConfiguration 1.
     * @param bCfg2 BinaryConfiguration 2.
     * @throws Exception If failed.
     */
    private void checkNegative(final BinaryConfiguration bCfg1, BinaryConfiguration bCfg2) throws Exception {
        binaryCfg = bCfg1;

        startGrid(0);

        binaryCfg = bCfg2;

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, "");

        isClient = true;

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(2);

                return null;
            }
        }, IgniteCheckedException.class, "");
    }

    /**
     * @return Custom BinaryConfiguration.
     * @param compactFooter Compact footer.
     */
    private BinaryConfiguration customConfig(boolean compactFooter) {
        BinaryConfiguration c = new BinaryConfiguration();

        c.setIdMapper(new BinaryBasicIdMapper(true));
        c.setSerializer(new BinarySerializer() {
            @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
                // No-op.
            }

            @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
                // No-op.
            }
        });
        c.setCompactFooter(compactFooter);

        BinaryTypeConfiguration btc = new BinaryTypeConfiguration("org.MyClass");

        btc.setIdMapper(BinaryContext.defaultIdMapper());
        btc.setEnum(false);
        btc.setSerializer(new BinarySerializer() {
            @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
                // No-op.
            }

            @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
                // No-op.
            }
        });

        c.setTypeConfigurations(Arrays.asList(btc));

        return c;
    }
}
