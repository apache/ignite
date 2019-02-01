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

import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * Test for {@link ExponentialBackoffSpiTimeoutTest}.
 */
@RunWith(JUnit4.class)
public class ExponentialBackoffSpiTimeoutTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void checkTimeoutFailureDetection() {
        ExponentialBackoffSpiTimeout helper = new ExponentialBackoffSpiTimeout(
            true,
            5_000L,
            1000L,
            3000L,
            3
        );

        checkTimeout(helper, 5_000L);
    }

    /** */
    @Test
    public void checkTimeoutNoFailureDetection() throws IOException, IgniteSpiOperationTimeoutException {
        ExponentialBackoffSpiTimeout helper = new ExponentialBackoffSpiTimeout(
            false,
            5_000L,
            1000L,
            3000L,
            3
        );

        int expDelay = 1000;

        assertEquals(1000L, helper.connTimeout());
        assertEquals(1000L, helper.handshakeTimeout());

        for (int i = 1; i < 3 && expDelay < 3000; i++)
            expDelay += Math.min(expDelay * 2, 3000);

        checkTimeout(helper, expDelay);
    }

    /** */
    @Test
    public void backoff() throws IgniteSpiOperationTimeoutException, IOException {
        ExponentialBackoffSpiTimeout helper = new ExponentialBackoffSpiTimeout(
            true,
            5_000L,
            1000L,
            3_000L,
            3
        );

        assertEquals(1000L, helper.handshakeTimeout());

        helper.backoffHandshakeTimeout();

        assertEquals(2000L, helper.handshakeTimeout());

        helper.backoffHandshakeTimeout();

        assertEquals(3000L, helper.handshakeTimeout());

        assertEquals(1000L, helper.connTimeout());

        helper.backoffConnTimeout();

        assertEquals(2000L, helper.connTimeout());

        helper.backoffConnTimeout();

        assertEquals(3000L, helper.connTimeout());

        assertEquals(200L, helper.getAndBackOffOutOfTopDelay());
        assertEquals(400L, helper.getAndBackOffOutOfTopDelay());
        assertEquals(800L, helper.getAndBackOffOutOfTopDelay());
        assertEquals(1600L, helper.getAndBackOffOutOfTopDelay());
        assertEquals(3000L, helper.getAndBackOffOutOfTopDelay());

        assertEquals(200L, helper.getAndBackoffReconnectDelay());
        assertEquals(400L, helper.getAndBackoffReconnectDelay());
        assertEquals(800L, helper.getAndBackoffReconnectDelay());
        assertEquals(1600L, helper.getAndBackoffReconnectDelay());
        assertEquals(3000L, helper.getAndBackoffReconnectDelay());
    }

    /** */
    private void checkTimeout(
        ExponentialBackoffSpiTimeout helper,
        long timeout
    ) {
        long start = System.currentTimeMillis();

        while (true) {
            boolean timedOut = helper.checkTimeout(0);

            if (timedOut) {
                assertTrue( (System.currentTimeMillis() + 100 - start) >= timeout);

                try {
                    helper.connTimeout();

                    fail("Should fail with IOException");
                } catch (IOException ignored) {
                    //No-op
                }

                try {
                    helper.handshakeTimeout();

                    fail("Should fail with IgniteSpiOperationTimeoutException");
                } catch (IgniteSpiOperationTimeoutException ignored) {
                    //No-op
                }

                return;
            }
        }


    }
}
