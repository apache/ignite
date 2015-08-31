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

package org.apache.ignite.internal.util.future.nio;

import org.apache.ignite.internal.util.nio.GridNioEmbeddedFuture;
import org.apache.ignite.internal.util.nio.GridNioFutureImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Test for NIO embedded future.
 */
public class GridNioEmbeddedFutureSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testNioEmbeddedFuture() throws Exception {
        // Original future.
        final GridNioFutureImpl<Integer> origFut = new GridNioFutureImpl<>();

        // Embedded future to test.
        final GridNioEmbeddedFuture<Integer> embFut = new GridNioEmbeddedFuture<>();

        embFut.onDone(origFut, null);

        assertFalse("Expect original future is not complete.", origFut.isDone());

        // Finish original future in separate thread.
        Thread t = new Thread() {
            @Override public void run() {
                origFut.onDone(100);
            }
        };

        t.start();
        t.join();

        assertTrue("Expect original future is complete.", origFut.isDone());
        assertTrue("Expect embedded future is complete.", embFut.isDone());

        // Wait for embedded future completes.
        assertEquals(new Integer(100), embFut.get(1, SECONDS));
    }
}