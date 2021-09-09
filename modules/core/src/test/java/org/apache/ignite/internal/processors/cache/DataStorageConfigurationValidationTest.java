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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.HALF_MAX_WAL_ARCHIVE_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Tests assertions in DataStorageConfiguration.
 */
public class DataStorageConfigurationValidationTest {
    /**
     * Tests {@link DataStorageConfiguration#getWalSegmentSize} property assertion.
     */
    @Test
    public void testWalSegmentSize() {
        DataStorageConfiguration cfg = new DataStorageConfiguration();

        for (int i : F.asList(1 << 31, 512 * 1024 - 1, 1))
            assertThrows(null, () -> cfg.setWalSegmentSize(i), IllegalArgumentException.class, null);

        for (int i : F.asList(512 * 1024, Integer.MAX_VALUE))
            assertEquals(i, cfg.setWalSegmentSize(i).getWalSegmentSize());
    }

    /**
     * Tests {@link DataStorageConfiguration#getMinWalArchiveSize} property assertion.
     */
    @Test
    public void testMinWalArchiveSize() {
        DataStorageConfiguration cfg = new DataStorageConfiguration();

        assertEquals(-1, HALF_MAX_WAL_ARCHIVE_SIZE);

        for (long i : F.asList(Long.MIN_VALUE, 0L))
            assertThrows(null, () -> cfg.setMinWalArchiveSize(i), IllegalArgumentException.class, null);

        for (long i : F.asList(1L, 100L, Long.MAX_VALUE, HALF_MAX_WAL_ARCHIVE_SIZE))
            assertEquals(i, cfg.setMinWalArchiveSize(i).getMinWalArchiveSize());
    }
}
