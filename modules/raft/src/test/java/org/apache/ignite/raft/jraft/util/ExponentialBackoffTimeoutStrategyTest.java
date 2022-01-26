/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Test for {@link ExponentialBackoffTimeoutStrategy}.
 */
public class ExponentialBackoffTimeoutStrategyTest {

    @Test
    public void testNextReset() {
        int initialTimeout = 2000;

        int maxTimeout = 11_000;

        int roundsWithoutAdjusting = 2;

        TimeoutStrategy timeoutStrategy = new ExponentialBackoffTimeoutStrategy(maxTimeout, roundsWithoutAdjusting);

        assertEquals(initialTimeout, timeoutStrategy.nextTimeout(initialTimeout, 1));
        assertEquals(initialTimeout, timeoutStrategy.nextTimeout(initialTimeout, 2));

        // default backoff coefficient equals to 2
        assertEquals(2 * initialTimeout, timeoutStrategy.nextTimeout(initialTimeout, 3));
        assertEquals(2 * initialTimeout, timeoutStrategy.nextTimeout(initialTimeout, 3));

        assertEquals(2 * initialTimeout, timeoutStrategy.nextTimeout(initialTimeout, 4));

        assertEquals(2 * 2 * initialTimeout, timeoutStrategy.nextTimeout(2 * initialTimeout, 4));

        assertEquals(maxTimeout, timeoutStrategy.nextTimeout(2 * 2 * initialTimeout, 4));

        assertEquals(maxTimeout, timeoutStrategy.nextTimeout(100_500, 4));
    }
}
