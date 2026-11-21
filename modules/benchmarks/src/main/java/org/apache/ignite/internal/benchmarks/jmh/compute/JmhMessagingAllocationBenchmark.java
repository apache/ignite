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

package org.apache.ignite.internal.benchmarks.jmh.compute;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.openjdk.jmh.annotations.Benchmark;

/** User messages from a client node to one server; each call waits until the server's listener has the message. */
public class JmhMessagingAllocationBenchmark extends JmhCacheAbstractBenchmark {
    /** Topic of the messages. */
    private static final String TOPIC = "jmh";

    /** Message. */
    private static final Integer MSG = 1_000_000;

    /** Longest wait for a message to arrive. */
    private static final long DELIVERY_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /** Messages the server's listener has received; the nodes run in this JVM. */
    private static final AtomicLong RCVD = new AtomicLong();

    /** Messaging of the client node to the first server. */
    private IgniteMessaging msgs;

    /** Value of {@link #RCVD} once the server has every message sent so far. */
    private long sent;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        Ignite srv = Ignition.ignite("node0");

        srv.message().localListen(TOPIC, (nodeId, msg) -> {
            RCVD.incrementAndGet();

            return true;
        });

        msgs = node.message(node.cluster().forNode(srv.cluster().localNode()));
        sent = RCVD.get();
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return true;
    }

    /** Sends a message to the first server. */
    @Benchmark
    public void send() {
        msgs.send(TOPIC, MSG);

        awaitDelivery();
    }

    /** Sends an ordered message to the first server. */
    @Benchmark
    public void sendOrdered() {
        msgs.sendOrdered(TOPIC, MSG, 0);

        awaitDelivery();
    }

    /** Waits for the server's listener to receive the message: unbounded sends pile up and break the connection. */
    private void awaitDelivery() {
        sent++;

        long deadline = System.nanoTime() + DELIVERY_TIMEOUT_NS;

        while (RCVD.get() < sent) {
            if (System.nanoTime() > deadline)
                throw new IllegalStateException("The server's listener didn't get message " + sent);

            Thread.onSpinWait();
        }
    }
}
