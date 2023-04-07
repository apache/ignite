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

package org.apache.ignite.util;

import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandList;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractCdcTest.KEYS_CNT;
import static org.apache.ignite.cdc.CdcSelfTest.addData;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.cdc.ResendCommand.CACHES;
import static org.apache.ignite.internal.commandline.cdc.ResendCommand.RESEND;
import static org.apache.ignite.testframework.GridTestUtils.stopThreads;
import static org.apache.ignite.util.CdcCommandTest.runCdc;
import static org.apache.ignite.util.CdcCommandTest.waitForSize;

/**
 * CDC resend command tests.
 */
public class CdcResendCommandTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(1000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)
                .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopThreads(log);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testResendCacheDataRestoreFromWal() throws Exception {
        IgniteEx ign = startGrid(0);

        ign.cluster().state(ACTIVE);

        enableCheckpoints(ign, false);

        addData(ign.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        AbstractCdcTest.UserCdcConsumer cnsmr = runCdc(ign);

        waitForSize(cnsmr, KEYS_CNT);

        cnsmr.clear();

        executeCommand(EXIT_CODE_OK, CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME);

        waitForSize(cnsmr, KEYS_CNT);

        stopAllGrids();

        ign = startGrid(0);

        assertEquals(KEYS_CNT, ign.cache(DEFAULT_CACHE_NAME).size());
    }
}
