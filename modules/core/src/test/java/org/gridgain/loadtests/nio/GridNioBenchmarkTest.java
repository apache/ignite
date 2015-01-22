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

package org.gridgain.loadtests.nio;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.*;
import org.apache.ignite.internal.util.nio.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.logger.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.nio.*;

/**
 *
 */
public class GridNioBenchmarkTest {
    /** */
    private final int port;

    /** */
    private final int selectorCnt;

    /**
     * @param selectorCnt Selector count.
     * @param port Port.
     */
    public GridNioBenchmarkTest(int selectorCnt, int port) {
        this.selectorCnt = selectorCnt;
        this.port = port;
    }

    /**
     * Runs the benchmark.
     *
     * @throws UnknownHostException If can't connect to given hist,
     * @throws IgniteCheckedException If NIO server initialisation failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void run() throws UnknownHostException, IgniteCheckedException {
        GridNioServerListener<ByteBuffer> lsnr = new GridNioServerListenerAdapter<ByteBuffer>() {
            @Override public void onConnected(GridNioSession ses) {
                X.print("New connection accepted.");
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                // No-op.
            }

            @Override public void onMessage(GridNioSession ses, ByteBuffer msg) {
                ByteBuffer buf = ByteBuffer.allocate(msg.remaining()).put(msg);
                buf.position(0);
                ses.send(buf);
            }

            @Override public void onSessionWriteTimeout(GridNioSession ses) {
                X.error("Session write timeout. Closing.");
            }

            @Override public void onSessionIdleTimeout(GridNioSession ses) {
                X.error("Session idle timeout. Closing.");
            }
        };

        IgniteLogger log  = new GridTestLog4jLogger(U.resolveGridGainUrl("config/gridgain-log4j.xml"));

        GridNioServer.<ByteBuffer>builder()
            .address(InetAddress.getByName("localhost"))
            .port(port)
            .listener(lsnr)
            .logger(log)
            .selectorCount(selectorCnt)
            .gridName("")
            .tcpNoDelay(false)
            .directBuffer(false)
            .byteOrder(ByteOrder.nativeOrder())
            .socketSendBufferSize(0)
            .socketReceiveBufferSize(0)
            .sendQueueLimit(0)
            .build()
            .start();
    }

    /**
     * Runs the benchmark.
     *
     * @param args Command line arguments.
     * @throws UnknownHostException If can't connect to given hist,
     * @throws IgniteCheckedException If NIO server initialisation failed.
     */
    public static void main(String[] args) throws UnknownHostException, IgniteCheckedException {
        if (args.length != 2) {
            X.println("Usage: " + GridNioBenchmarkTest.class.getSimpleName() + " <threads> <port>");

            return;
        }

        final int threads = Integer.parseInt(args[0]);
        final int port = Integer.parseInt(args[1]);

        new GridNioBenchmarkTest(threads, port).run();
    }
}
