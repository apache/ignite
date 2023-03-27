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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE;
import static org.apache.ignite.configuration.DataStorageConfiguration.HALF_MAX_WAL_ARCHIVE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.minWalArchiveSize;

/**
 * Class for testing {@link FileWriteAheadLogManager}.
 */
public class FileWriteAheadLogManagerSelfTest extends GridCommonAbstractTest {
    /**
     * Testing of {@link FileWriteAheadLogManager#minWalArchiveSize(DataStorageConfiguration)}.
     */
    @Test
    @WithSystemProperty(key = IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, value = "-1")
    public void testGettingMinWalArchiveSizeFromConfiguration() {
        DataStorageConfiguration cfg = new DataStorageConfiguration().setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE);

        for (long i : F.asList(10L, 20L, HALF_MAX_WAL_ARCHIVE_SIZE))
            assertEquals(UNLIMITED_WAL_ARCHIVE, minWalArchiveSize(cfg.setMinWalArchiveSize(i)));

        cfg.setMaxWalArchiveSize(100);

        for (long i : F.asList(10L, 20L))
            assertEquals(i, minWalArchiveSize(cfg.setMinWalArchiveSize(i)));

        assertEquals(50, minWalArchiveSize(cfg.setMinWalArchiveSize(HALF_MAX_WAL_ARCHIVE_SIZE)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        clearProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE);
    }

    /**
     * Testing of {@link FileWriteAheadLogManager#minWalArchiveSize(DataStorageConfiguration)}.
     */
    @Test
    public void testGettingMinWalArchiveSizeFromSystemProperty() {
        DataStorageConfiguration cfg = new DataStorageConfiguration()
            .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE)
            .setMinWalArchiveSize(HALF_MAX_WAL_ARCHIVE_SIZE);

        for (double i : F.asList(0.1d, 0.3d, (double)HALF_MAX_WAL_ARCHIVE_SIZE)) {
            setProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, Double.toString(i));
            assertEquals(UNLIMITED_WAL_ARCHIVE, minWalArchiveSize(cfg));
        }

        int max = 100;
        cfg.setMaxWalArchiveSize(max);

        for (double i : F.asList(0.1d, 0.2d)) {
            setProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, Double.toString(i));
            assertEquals((long)(i * max), minWalArchiveSize(cfg));
        }

        setProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, Double.toString(0.5));
        assertEquals(25, minWalArchiveSize(cfg.setMinWalArchiveSize(25)));
    }
}
