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

package org.apache.ignite.spi.checkpoint.cache;

import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks correct checkpoint times logging
 */
public class CheckCheckpointStartLoggingTest extends GridCommonAbstractTest {
    /** */
    private static final String VALID_MS_PATTERN = "[0-9]*ms";

    /** */
    private static final String CHECKPOINT_STARTED_LOG_FORMAT = "Checkpoint started .*" +
        "checkpointBeforeLockTime=" + VALID_MS_PATTERN + ", " +
        "checkpointLockWait=" + VALID_MS_PATTERN + ", " +
        "checkpointListenersExecuteTime="+ VALID_MS_PATTERN +", " +
        "checkpointLockHoldTime="+ VALID_MS_PATTERN + ", " +
        "walCpRecordFsyncDuration="+ VALID_MS_PATTERN +", " +
        "writeCheckpointEntryDuration="+ VALID_MS_PATTERN +", " +
        "splitAndSortCpPagesDuration="+ VALID_MS_PATTERN +", " +
        ".* pages=[1-9][0-9]*, " +
        "reason=.*";

    /** */
    private ListeningTestLogger testLogger = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(10 * 1024 * 1024)
                    .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setGridLogger(testLogger);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCheckpointLogging() throws Exception {
        LogListener lsnr = LogListener.matches(Pattern.compile(CHECKPOINT_STARTED_LOG_FORMAT)).build();

        testLogger.registerListener(lsnr);

        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        forceCheckpoint();

        assertTrue(lsnr.check());
    }
}
