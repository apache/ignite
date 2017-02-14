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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;

/**
 * This streamer uses https://github.com/zeromq/jeromq/.
 */
public class IgniteZeroMqStreamer<K, V> extends StreamAdapter<byte[], K, V> implements AutoCloseable {
    /** Logger. */
    protected IgniteLogger log;

    /** Threads count used to transform zeromq message. */
    private int threadsCount = 1;

    /** Parametr {@code True} if streamer started. */
    private boolean isStart = false;

    /** Process stream asynchronously */
    private ExecutorService zeroMqExSrv;

    /**  */
    private ZMQ.Context ctx;

    /**  */
    private ZMQ.Socket socket;

    /** */
    private int ioThreads;

    /** */
    private int socketType;

    /** */
    private String addr;

    /** */
    private byte[] topic;

    /**
     * @param ioThreads Threads on context.
     * @param socketType Socket type.
     * @param addr Address to connect zmq.
     */
    public IgniteZeroMqStreamer(int ioThreads, ZeroMqTypeSocket socketType, @NotNull String addr, byte[] topic) {
        A.ensure(ioThreads > 0, "Param ioThreads.");
        A.ensure(!"".equals(addr), "Param addr.");

        this.ioThreads = ioThreads;
        this.addr = addr;
        this.topic = topic;

        if (ZeroMqTypeSocket.check(socketType))
            this.socketType = socketType.getType();
        else
            throw new IgniteException("This socket type not implementation this version ZeroMQ streamer.");
    }

    /**
     * Sets Threads count.
     *
     * @param threadsCount Threads count.
     */
    private void setThreadsCount(int threadsCount) {
        assert threadsCount > 0;

        this.threadsCount = threadsCount;
    }

    /**
     * Start ZeroMQ streamer.
     */
    public void start() {
        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null, "ZeroMq extractor.");

        log = getIgnite().log();

        if (isStart) {
            log.info("Attempted to start an already started ZeroMQ streamer");
            return;
        }

        isStart = true;

        ctx = ZMQ.context(ioThreads);
        socket = ctx.socket(socketType);
        socket.connect(addr);

        if (ZeroMqTypeSocket.SUB.getType() == socketType)
            socket.subscribe(topic);

        zeroMqExSrv = Executors.newFixedThreadPool(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            Callable<Boolean> task = new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    while (true) {
                        if (ZeroMqTypeSocket.SUB.getType() == socketType)
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
    @Override public void close() throws Exception {
        socket.close();
        ctx.close();

        zeroMqExSrv.shutdownNow();

        isStart = false;
    }
}
