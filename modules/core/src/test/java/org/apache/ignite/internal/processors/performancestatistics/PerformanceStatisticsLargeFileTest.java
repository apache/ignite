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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.systemview.ConfigurationViewWalker;
import org.apache.ignite.spi.systemview.view.ConfigurationView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Manual test to generate a large system views performance statistics file without heavy load.
 */
public class PerformanceStatisticsLargeFileTest extends AbstractPerformanceStatisticsTest {
    /** Target performance statistics file size (about 0.5 GB). */
    private static final long TARGET_FILE_SIZE = 512L * 1024 * 1024;

    /** Allowed size deviation to account for other system views. */
    private static final long SIZE_TOLERANCE = 32L * 1024 * 1024;

    /** Timeout for system view writer completion. */
    private static final long SYSTEM_VIEW_TIMEOUT = 300_000;

    /** Name length to inflate record sizes. */
    private static final int NAME_LEN = 256;

    /** Value length to inflate record sizes. */
    private static final int VALUE_LEN = 4 * 1024;

    /** Estimated record size for a system view row entry including the operation type byte. */
    private static final int ROW_RECORD_SIZE = 1 /* op */
        + 1 /* cached flag */ + 4 /* length */ + NAME_LEN
        + 1 /* cached flag */ + 4 /* length */ + VALUE_LEN;

    /** Number of records needed to reach the target size. */
    private static final int ROW_COUNT = 1_000_000;

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

        /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Do not remove perfStat dir
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPerformanceStatisticsDir();
    }

    /**
     * Generates a large system views performance statistics file around 0.5 GB in size.
     */
    @Test
    public void testGenerateLargeSystemViewStatisticsFile() throws Exception {
        LogListener lsnr = LogListener.matches("Finished writing system views to performance statistics file:")
            .build();
        listeningLog.registerListener(lsnr);

        IgniteEx ignite = startGrid(0);

        ignite.context().systemView().registerView(new LargeSystemView(ROW_COUNT));

        startCollectStatistics();

        assertTrue("Performance statistics writer did not finish.",
            waitForCondition(lsnr::check, SYSTEM_VIEW_TIMEOUT));

        stopCollectStatistics();

        System.out.println("SAVED TO" + statisticsFiles());

        List<File> files = systemViewStatisticsFiles(statisticsFiles());
        long totalSize = files.stream().mapToLong(File::length).sum();

        assertTrue("Generated file size is below target: " + totalSize, totalSize >= TARGET_FILE_SIZE);
    }

    /** @return Random string with a fixed length. */
    private static String randomString(int len) {
        char[] chars = new char[len];
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < len; i++)
            chars[i] = (char)('a' + random.nextInt(26));

        return new String(chars);
    }

    /** System view with large row payloads to inflate file size. */
    private static class LargeSystemView implements SystemView<ConfigurationView> {
        /** View name. */
        private static final String VIEW_NAME = "performance.statistics.large.system.view";

        /** View description. */
        private static final String VIEW_DESC = "Large system view for performance statistics file size test.";

        /** Row count. */
        private final int rowCnt;

        /** Row walker. */
        private final SystemViewRowAttributeWalker<ConfigurationView> walker = new ConfigurationViewWalker();

        /** @param rowCnt Row count. */
        LargeSystemView(int rowCnt) {
            this.rowCnt = rowCnt;
        }

        /** {@inheritDoc} */
        @Override public SystemViewRowAttributeWalker<ConfigurationView> walker() {
            return walker;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return VIEW_NAME;
        }

        /** {@inheritDoc} */
        @Override public String description() {
            return VIEW_DESC;
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return rowCnt;
        }

        /** {@inheritDoc} */
        @Override public Iterator<ConfigurationView> iterator() {
            return new Iterator<ConfigurationView>() {
                private int idx;

                @Override public boolean hasNext() {
                    return idx < rowCnt;
                }

                @Override public ConfigurationView next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    idx++;

                    return new ConfigurationView(randomString(NAME_LEN), randomString(VALUE_LEN));
                }
            };
        }
    }
}
