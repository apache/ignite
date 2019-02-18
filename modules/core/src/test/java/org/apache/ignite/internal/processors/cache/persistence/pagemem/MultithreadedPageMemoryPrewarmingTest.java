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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PrewarmingConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class MultithreadedPageMemoryPrewarmingTest extends PageMemoryPrewarmingTest {
    /** Count of threads which will be used for warm up pages loading into memory. */
    protected int pageLoadThreads = 4;

    /** Count of threads which will be used for warm up dump files reading. */
    protected int dumpReadThreads = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024 * 1024)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(maxMemorySize)
                .setInitialSize(maxMemorySize)
                .setPersistenceEnabled(true)
                .setPrewarmingConfiguration(new PrewarmingConfiguration()
                    .setWaitPrewarmingOnStart(waitPrewarmingOnStart)
                    .setRuntimeDumpDelay(prewarmingRuntimeDumpDelay)
                    .setPageLoadThreads(pageLoadThreads)
                    .setDumpReadThreads(dumpReadThreads))
            );

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }
}
