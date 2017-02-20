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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;

/**
 * This streamer uses https://github.com/zeromq/jeromq/.
 * Implements socket types listed {@link ZeroMqTypeSocket}.
 */
public class IgniteZeroMqStreamer<K, V> extends StreamAdapter<byte[], K, V> implements AutoCloseable {
    /** Logger. */
    protected IgniteLogger log;

    /** Parameter {@code True} if streamer started. */
    private volatile boolean isStarted = false;

    /** Process stream asynchronously */
    private ExecutorService zeroMqExSrv;

    /** ZeroMQ context. */
    private ZMQ.Context ctx;

    /** ZeroMQ socket. */
    private ZMQ.Socket socket;

    /** ZeroMQ context threads. */
    private int ioThreads;

    /** ZeroMQ socket type. */
    private int socketType;

    /** ZeroMQ socket address.*/
    private String addr;

    /** ZeroMQ topic name. */
    private byte[] topic;

    /** Maximum time to wait (time unit minutes). */
    private int timeout = 2;

    /**
     * @param ioThreads Threads on context.
     * @param socketType Socket type.
     * @param addr Address to connect zmq.
     * @param topic Topic name for PUB-SUB socket type, otherwise null.
     */
    public IgniteZeroMqStreamer(int ioThreads, ZeroMqTypeSocket socketType, @NotNull String addr, byte[] topic) {
        A.ensure(ioThreads > 0, "Param ioThreads has been more than 0.");
        A.ensure(!"".equals(addr), "Param addr has been not empty.");
        A.ensure(socketType != null, "This socket type is not implemented in this version of ZeroMQ streamer.");

        this.ioThreads = ioThreads;
        this.addr = addr;
        this.topic = topic;
        this.socketType = socketType.getType();
    }

    /**
     * Start ZeroMQ streamer.
     */
    public void start() {
        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null, "ZeroMq extractor.");

        log = getIgnite().log();

        if (isStarted) {
            log.warning("Attempted to start an already started ZeroMQ streamer");
            return;
        }

        isStarted = true;

        ctx = ZMQ.context(ioThreads);
        socket = ctx.socket(socketType);
        socket.connect(addr);

        if (ZeroMqTypeSocket.SUB.getType() == socketType)
            socket.subscribe(topic);

        zeroMqExSrv = Executors.newSingleThreadExecutor();

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

    /**
     * Stop ZeroMQ streamer.
     */
    @Override public void close() throws Exception {
        socket.close();
        ctx.close();

        zeroMqExSrv.shutdown();
        timeout = 1;
        zeroMqExSrv.awaitTermination(timeout, TimeUnit.MINUTES);

        isStarted = false;
    }
}
