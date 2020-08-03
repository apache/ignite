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

package org.apache.ignite.spi;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link ExponentialBackoffTimeoutStrategyTest}.
 */
public class ExponentialBackoffTimeoutStrategyTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void checkTimeout() {
        ExponentialBackoffTimeoutStrategy helper = new ExponentialBackoffTimeoutStrategy(
            5_000L,
            1000L,
            3000L
        );

        checkTimeout(helper, 5_000L);
    }

    /** */
    @Test
    public void backoff() throws IgniteSpiOperationTimeoutException {
        ExponentialBackoffTimeoutStrategy strategy = new ExponentialBackoffTimeoutStrategy(
            25_000L,
            1000L,
            3_000L
        );

        assertEquals(1000L, strategy.nextTimeout());

        assertEquals(1000L, strategy.nextTimeout(1000L));

        assertEquals(2000L, strategy.nextTimeout());

        assertEquals(1000L, strategy.nextTimeout(1000L));

        assertEquals(2000L, strategy.nextTimeout(2000L));

        assertEquals(3000L, strategy.nextTimeout());

        assertEquals(3000L, strategy.nextTimeout());

        assertEquals(100L, strategy.nextTimeout(100L));
    }

    /** */
    @Test
    public void totalBackoffTimeout() {
        assertEquals(8_000, ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(1000, 5000, 3));
        assertEquals(45_000, ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(5_000, 60_000, 3));
    }

    /** */
    private void checkTimeout(
        ExponentialBackoffTimeoutStrategy strategy,
        long timeout
    ) {
        long start = System.currentTimeMillis();

        while (true) {
            boolean timedOut = strategy.checkTimeout();

            if (timedOut) {
                assertTrue( (System.currentTimeMillis() + 100 - start) >= timeout);

                try {
                    strategy.nextTimeout();

                    fail("Should fail with IgniteSpiOperationTimeoutException");
                } catch (IgniteSpiOperationTimeoutException ignored) {
                    //No-op
                }

                return;
            }
        }
    }
}
