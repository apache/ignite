/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.concurrent.TimeUnit;
import com.google.common.base.Strings;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Puts data into grid, waits for checkpoint to start and then verifies data
 */
public class IgnitePdsCheckpointSimpleTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration regCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(4 * 1024)
            .setDefaultDataRegionConfiguration(regCfg)
            .setCheckpointFrequency(TimeUnit.SECONDS.toMillis(10));

        return cfg.setDataStorageConfiguration(dsCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks if same data can be loaded after checkpoint.
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryAfterCpEnd() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.active(true);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("cache");

        for (int i = 0; i < 10000; i++)
            cache.put(i, valueWithRedundancyForKey(i));

        ignite.context().cache().context().database().waitForCheckpoint("test");

        stopAllGrids();

        IgniteEx igniteRestart = startGrid(0);
        igniteRestart.active(true);

        IgniteCache<Object, Object> cacheRestart = igniteRestart.getOrCreateCache("cache");

        for (int i = 0; i < 10000; i++)
            assertEquals(valueWithRedundancyForKey(i), cacheRestart.get(i));

        stopAllGrids();
    }

    /**
     * @param i key.
     * @return value with extra data, which allows to verify
     */
    private @NotNull String valueWithRedundancyForKey(int i) {
        return Strings.repeat(Integer.toString(i), 10);
    }
}
