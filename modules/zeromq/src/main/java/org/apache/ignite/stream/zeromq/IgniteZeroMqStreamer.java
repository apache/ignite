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

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.zeromq.ZMQ;

/**
 * This streamer uses https://github.com/zeromq/jeromq/.
 */
public class IgniteZeroMqStreamer<K, V> extends StreamAdapter<byte[], K, V> {
    /** Logger. */
    protected IgniteLogger log;

    /**  */
    private ZeroMqSettings zeroMqSettings;

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

    /** Counter. */
    private AtomicInteger count = new AtomicInteger();

    /**
     *
     */
    public IgniteZeroMqStreamer(ZeroMqSettings zeroMqSettings) {
        this.zeroMqSettings = zeroMqSettings;

        count.set(0);

        StreamSingleTupleExtractor singleTupleExtractor = new DefaultSingleTupleExtractorImpl();

        setSingleTupleExtractor(singleTupleExtractor);
    }

    /**
     * Sets Threads count.
     *
     * @param threadsCount Threads count.
     */
    public void setThreadsCount(int threadsCount) {
        this.threadsCount = threadsCount;
    }

    /**
     * Start ZeroMQ streamer.
     */
    public void start() {
        if (isStart)
            throw new IgniteException("Attempted to start an already started ZeroMQ streamer");

        isStart = true;

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
        if (!isStart)
            throw new IgniteException("Attempted to stop an already stopped ZeroMQ streamer");

        socket.close();
        ctx.close();

        zeroMqExSrv.shutdownNow();

        isStart = false;
    }

    /**
     * Default implementation single tuple extractor for ZeroMQ streamer.
     */
    class DefaultSingleTupleExtractorImpl implements StreamSingleTupleExtractor<byte[], Integer, String> {
        @Override public Map.Entry<Integer, String> extract(byte[] msg) {
            try {
                return new IgniteBiTuple<>(count.getAndIncrement(), new String(msg, Charset.forName("UTF-8")));
            }
            catch (IgniteException e) {
                U.error(log, e);

                return null;
            }
        }
    }


}
