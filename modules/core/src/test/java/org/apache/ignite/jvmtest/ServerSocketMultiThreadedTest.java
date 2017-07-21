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

package org.apache.ignite.jvmtest;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Java server socket test.
 *
 * When binding server socket to the same port in multiple threads, either
 * BindException or SocketException may be thrown. Purpose of this test is
 * to find some explanation to that.
 */
public class ServerSocketMultiThreadedTest extends TestCase {
    /** */
    private static final int THREADS_CNT = 10;

    /** */
    private static final int ITER_CNT = 10000;

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentBind() throws Exception {
        final AtomicInteger bindExCnt = new AtomicInteger();
        final AtomicInteger sockExCnt = new AtomicInteger();
        final AtomicInteger okCnt = new AtomicInteger();

        final CyclicBarrier finishBarrier = new CyclicBarrier(
            THREADS_CNT,
            new Runnable() {
                private int i;

                @Override public void run() {
                    if (++i % 250 == 0)
                        X.println("Finished iteration [threadName=" + Thread.currentThread().getName() +
                            ", iter=" + i + ']');
                }
            });

        final InetAddress addr = InetAddress.getByName("127.0.0.1");

        GridTestUtils.runMultiThreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ServerSocket srvSock = null;

                    for (int i = 0; i < ITER_CNT; i++) {
                        try {
                            srvSock = new ServerSocket(60000, 0, addr);

                            okCnt.incrementAndGet();
                        }
                        catch (BindException ignore) {
                            bindExCnt.incrementAndGet();
                        }
                        catch (SocketException ignore) {
                            sockExCnt.incrementAndGet();
                        }
                        finally {
                            finishBarrier.await();

                            U.closeQuiet(srvSock);
                        }
                    }

                    return null;
                }
            },
            THREADS_CNT,
            "binder"
        );

        X.println("Test stats [bindExCnt=" + bindExCnt.get() + ", sockExCnt=" + sockExCnt.get() +
            ", okCnt=" + okCnt + ']');
    }
}