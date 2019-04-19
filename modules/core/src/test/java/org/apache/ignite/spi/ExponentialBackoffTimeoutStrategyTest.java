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

package org.apache.ignite.spi;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link ExponentialBackoffTimeoutStrategyTest}.
 */
@RunWith(JUnit4.class)
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
