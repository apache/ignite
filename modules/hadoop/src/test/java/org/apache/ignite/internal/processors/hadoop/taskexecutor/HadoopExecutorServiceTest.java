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

package org.apache.ignite.internal.processors.hadoop.taskexecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.LongAdder8;

/**
 *
 */
public class HadoopExecutorServiceTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testExecutesAll() throws Exception {
        final HadoopExecutorService exec = new HadoopExecutorService(log, "_GRID_NAME_", 10, 5);

        for (int i = 0; i < 5; i++) {
            final int loops = 5000;
            int threads = 17;

            final LongAdder8 sum = new LongAdder8();

            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < loops; i++) {
                        exec.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                sum.increment();

                                return null;
                            }
                        });
                    }

                    return null;
                }
            }, threads);

            while (exec.active() != 0) {
                X.println("__ active: " + exec.active());

                Thread.sleep(200);
            }

            assertEquals(threads * loops, sum.sum());

            X.println("_ ok");
        }

        assertTrue(exec.shutdown(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testShutdown() throws Exception {
        for (int i = 0; i < 5; i++) {
            final HadoopExecutorService exec = new HadoopExecutorService(log, "_GRID_NAME_", 10, 5);

            final LongAdder8 sum = new LongAdder8();

            final AtomicBoolean finish = new AtomicBoolean();

            IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finish.get()) {
                        exec.submit(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                sum.increment();

                                return null;
                            }
                        });
                    }

                    return null;
                }
            }, 19);

            Thread.sleep(200);

            assertTrue(exec.shutdown(50));

            long res = sum.sum();

            assertTrue(res > 0);

            finish.set(true);

            fut.get();

            assertEquals(res, sum.sum()); // Nothing was executed after shutdown.

            X.println("_ ok");
        }
    }
}