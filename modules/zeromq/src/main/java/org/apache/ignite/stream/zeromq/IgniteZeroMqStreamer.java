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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /**
     * Size of buffer for streaming, as for some tracking terms traffic can be low and for others high, this is
     * configurable
     */
    private Integer bufferCapacity = 100000;

    /** Process stream asynchronously */
    private ExecutorService zeroMqExServ;

    private ZMQ.Context ctx;
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
            throw new IgniteException("Attempted to start an already started ZeroMQ Streamer");

        checkSettings();

        log = getIgnite().log();

        ctx = ZMQ.context(zeroMqSettings.getIoThreads());
        socket = ctx.socket(zeroMqSettings.getType());
        socket.connect(zeroMqSettings.getAddr());

        final BlockingQueue<byte[]> zeroMqMsgQueue = new LinkedBlockingQueue<>(bufferCapacity);

        zeroMqExServ = Executors.newFixedThreadPool(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            Callable<Boolean> task = new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    while (true) {
                        try {
                            byte[] msg = zeroMqMsgQueue.take();

                            addMessage(msg);
                        }
                        catch (InterruptedException e) {
                            U.warn(log, "ZeroMQ message transformation was interrupted", e);

                            return true;
                        }
                    }
                }
            };

            zeroMqExServ.submit(task);
        }
    }

    /**
     * Stop ZeroMQ streamer.
     */
    public void stop() {
        socket.close();
        ctx.close();

        isStart.set(false);
    }

    /**
     *
     */
    private boolean checkSettings() {
        // TODO in progress
        return false;
    }

    /**
     * Sets Buffer capacity.
     *
     * @param bufferCapacity Buffer capacity.
     */
    public void setBufferCapacity(Integer bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
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
