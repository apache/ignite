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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Allow to connect to addresses parallel.
 */
class SocketMultiConnector implements AutoCloseable {
    /** */
    private int connInProgress;

    /** */
    private final ExecutorService executor;

    /** */
    private final CompletionService<GridTuple3<InetSocketAddress, Socket, Exception>> completionSrvc;

    /**
     * @param spi Discovery SPI.
     * @param addrs Addresses.
     * @param retryCnt Retry count.
     */
    SocketMultiConnector(final TcpDiscoverySpi spi, Collection<InetSocketAddress> addrs,
        final int retryCnt) {
        connInProgress = addrs.size();

        executor = Executors.newFixedThreadPool(Math.min(1, addrs.size()));

        completionSrvc = new ExecutorCompletionService<>(executor);

        for (final InetSocketAddress addr : addrs) {
            completionSrvc.submit(new Callable<GridTuple3<InetSocketAddress, Socket, Exception>>() {
                @Override public GridTuple3<InetSocketAddress, Socket, Exception> call() {
                    Exception ex = null;
                    Socket sock = null;

                    for (int i = 0; i < retryCnt; i++) {
                        if (Thread.currentThread().isInterrupted())
                            return null; // Executor is shutdown.

                        try {
                            sock = spi.openSocket(addr);

                            break;
                        }
                        catch (Exception e) {
                            ex = e;
                        }
                    }

                    return new GridTuple3<>(addr, sock, ex);
                }
            });
        }
    }

    /**
     *
     */
    @Nullable public GridTuple3<InetSocketAddress, Socket, Exception> next() {
        if (connInProgress == 0)
            return null;

        try {
            Future<GridTuple3<InetSocketAddress, Socket, Exception>> fut = completionSrvc.take();

            connInProgress--;

            return fut.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteSpiException("Thread has been interrupted.", e);
        }
        catch (ExecutionException e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        List<Runnable> unstartedTasks = executor.shutdownNow();

        connInProgress -= unstartedTasks.size();

        if (connInProgress > 0) {
            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        executor.awaitTermination(5, TimeUnit.MINUTES);

                        Future<GridTuple3<InetSocketAddress, Socket, Exception>> fut;

                        while ((fut = completionSrvc.poll()) != null) {
                            try {
                                GridTuple3<InetSocketAddress, Socket, Exception> tuple3 = fut.get();

                                if (tuple3 != null)
                                    IgniteUtils.closeQuiet(tuple3.get2());
                            }
                            catch (ExecutionException ignore) {

                            }
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new RuntimeException(e);
                    }
                }
            });

            thread.setDaemon(true);

            thread.start();
        }
    }
}
