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

package org.apache.ignite.util;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;
import org.jsr166.ConcurrentLinkedDeque8.Node;
import org.junit.Test;

/**
 * Test for {@link org.jsr166.ConcurrentLinkedDeque8}.
 */
public class GridConcurrentLinkedDequeMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    private static final Random RND = new Random();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueMultiThreaded() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        final ConcurrentLinkedDeque8<Byte> queue = new ConcurrentLinkedDeque8<>();

        // Poll thread.
        IgniteInternalFuture<?> pollFut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Thread started.");

                    while (!done.get())
                        try {
                            queue.poll();
                        }
                        catch (Throwable t) {
                            error("Error in poll thread.", t);

                            done.set(true);
                        }

                    info("Thread finished.");

                    return null;
                }
            },
            5,
            "queue-poll"
        );

        // Producer thread.
        IgniteInternalFuture<?> prodFut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Thread started.");

                    while (!done.get()) {
                        Node<Byte> n = queue.addx((byte)1);

                        if (RND.nextBoolean())
                            queue.unlinkx(n);
                    }

                    info("Thread finished.");

                    return null;
                }
            },
            5,
            "queue-prod"
        );

        Thread.sleep(20 * 1000);

        done.set(true);

        pollFut.get();
        prodFut.get();
    }
}
