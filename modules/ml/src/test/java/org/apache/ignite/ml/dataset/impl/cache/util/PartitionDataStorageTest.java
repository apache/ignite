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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PartitionDataStorage}.
 */
public class PartitionDataStorageTest {
    /** Data storage. */
    private PartitionDataStorage dataStorage = new PartitionDataStorage();

    /** Tests {@code computeDataIfAbsent()} method. */
    @Test
    public void testComputeDataIfAbsent() {
        AtomicLong cnt = new AtomicLong();

        for (int i = 0; i < 10; i++) {
            Integer res = dataStorage.computeDataIfAbsent(0, () -> {
                cnt.incrementAndGet();

                return 42;
            });

            assertEquals(42, res.intValue());
        }

        assertEquals(1, cnt.intValue());
    }
}
