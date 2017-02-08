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

package org.apache.ignite.stream.zeromq;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.stream.StreamAdapter;
import org.zeromq.ZMQ;

/**
 *
 */
public class IgniteZeroMqStreamer<K, V> extends StreamAdapter<byte[], K, V> {
    /** Logger. */
    protected IgniteLogger log;

    /**  */
    private ZeroMqSettings zeroMqSettings;

    /** Threads count used to transform zeromq message. */
    private int threadsCount = 1;

    /** Parametr {@code True} if streamer started. */
    private static AtomicBoolean isStart = new AtomicBoolean();

    /** Process stream asynchronously */
    private ExecutorService zeroMqExSrv;

    /**  */
    private ZMQ.Context ctx;

    /**  */
    private ZMQ.Socket socket;

    /**
     *
     */
    public IgniteZeroMqStreamer(ZeroMqSettings zeroMqSettings) {
        this.zeroMqSettings = zeroMqSettings;
    }

    /**
     * Start ZeroMQ streamer.
     */
    public void start() {
        if (!isStart.compareAndSet(false, true))
            throw new IgniteException("Attempted to start an already started ZeroMQ streamer");

        log = getIgnite().log();

        ctx = ZMQ.context(zeroMqSettings.getIoThreads());
        socket = ctx.socket(zeroMqSettings.getType());
        socket.connect(zeroMqSettings.getAddr());

        if (ZeroMqTypeSocket.SUB.getType() == zeroMqSettings.getType())
            socket.subscribe(zeroMqSettings.getTopic());

        zeroMqExSrv = Executors.newFixedThreadPool(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            Callable<Boolean> task = new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    while (true) {
                        if (ZeroMqTypeSocket.SUB.getType() == zeroMqSettings.getType())
                            socket.recv();
                        addMessage(socket.recv());
                    }
                }
            };

            zeroMqExSrv.submit(task);
        }
    }

    /**
     * Stop ZeroMQ streamer.
     */
    public void stop() {
        if (!isStart.get())
            throw new IgniteException("Attempted to stop an already stopped ZeroMQ streamer");

        zeroMqExSrv.shutdownNow();

        socket.close();
        ctx.close();

        isStart.set(false);
    }

    /**
     * Sets Threads count.
     *
     * @param threadsCount Threads count.
     */
    public void setThreadsCount(int threadsCount) {
        this.threadsCount = threadsCount;
    }
}
