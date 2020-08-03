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

import java.util.concurrent.Callable;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests assertions in DataStorageConfiguration.
 */
public class DataStorageConfigurationValidationTest {
    /**
     * Tests {@link DataStorageConfiguration#walSegmentSize} property assertion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWalSegmentSizeOverflow() throws Exception {
        final DataStorageConfiguration cfg = new DataStorageConfiguration();

        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() {
                cfg.setWalSegmentSize(1 << 31);

                return null;
            }
        }, IllegalArgumentException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetWalSegmentSizeShouldThrowExceptionWhenSizeLessThen512Kb() throws Exception {
        final DataStorageConfiguration cfg = new DataStorageConfiguration();

        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cfg.setWalSegmentSize(512 * 1024 - 1);

                return null;
            }
        }, IllegalArgumentException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetWalSegmentSizeShouldBeOkWhenSizeBetween512KbAnd2Gb() throws Exception {
        final DataStorageConfiguration cfg = new DataStorageConfiguration();

        cfg.setWalSegmentSize(512 * 1024);

        assertEquals(512 * 1024, cfg.getWalSegmentSize());

        cfg.setWalSegmentSize(Integer.MAX_VALUE);

        assertEquals(Integer.MAX_VALUE, cfg.getWalSegmentSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetWalSegmentsCountShouldThrowExceptionThenLessThan2() throws Exception {
        final DataStorageConfiguration cfg = new DataStorageConfiguration();

        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cfg.setWalSegments(1);

                return null;
            }
        }, IllegalArgumentException.class, null);
    }

}
