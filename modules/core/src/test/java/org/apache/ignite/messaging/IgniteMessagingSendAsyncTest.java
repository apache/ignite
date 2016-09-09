/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.ignite.messaging;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by dgovorukhin on 09.09.2016.
 */
public class IgniteMessagingSendAsyncTest extends GridCommonAbstractTest {
    /**
     * Ignite instance for test.
     */
    private Ignite ignite;

    /**
     * Topic name.
     */
    private final String TOPIC = "topic";

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite = startGrid(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
        super.afterTest();

        stopGrid(0);
    }

    /**
     * Test for check, that if use default mode, local listeners execute
     * in the same thread.
     */
    public void testSendDefaultMode() throws InterruptedException {
        final String msgStr = "message";

        send(ignite.message(), msgStr, new ProcedureApply() {
            @Override
            public void apply(String msg, Thread thread) {
                Assert.assertEquals(Thread.currentThread(), thread);
                Assert.assertEquals(msgStr, msg);
            }
        });

    }

    /**
     * Test for check, that if use async mode, local listeners execute
     * in another thread(through pool).
     */
    public void testSendAsyncMode() throws InterruptedException {
        final String msgStr = "message";

        send(ignite.message().withAsync(), msgStr, new ProcedureApply() {
            @Override
            public void apply(String msg, Thread thread) {
                Assert.assertTrue(!Thread.currentThread().equals(thread));
                Assert.assertEquals(msgStr, msg);
            }
        });
    }

    /**
     * @param igniteMsg Ignite message.
     * @param msgStr    Message string.
     * @param cls       callback for compare result.
     */
    private void send(
            IgniteMessaging igniteMsg,
            String msgStr,
            ProcedureApply cls
    ) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<Thread> thread = new AtomicReference<>();
        final AtomicReference<String> val = new AtomicReference<>();

        igniteMsg.localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override
            public boolean apply(UUID uuid, String msgStr) {
                thread.set(Thread.currentThread());
                val.set(msgStr);
                latch.countDown();
                return false;
            }
        });

        igniteMsg.send(TOPIC, msgStr);

        latch.await();

        cls.apply(val.get(), thread.get());
    }

    /**
     * only for this test procedure
     */
    private interface ProcedureApply {

        /**
         * @param val    Value.
         * @param thread Thread.
         */
        void apply(String val, Thread thread);
    }

}
