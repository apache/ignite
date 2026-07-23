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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;

/** Communication SPI test message. */
public class IgniteIoTestMessage implements MarshallableMessage {
    /** Test ID. */
    @Order(0)
    long id;

    /** Process message in NIO thread. */
    @Order(1)
    boolean procFromNioThread;

    /** Request flag. */
    @Order(2)
    boolean req;

    /** Payload. */
    @Order(3)
    byte[] payload;

    /** Request creation timestamp from the source node monotonic clock. */
    @Order(4)
    long reqCreateTs;

    /** Request serialization-start timestamp from the source node monotonic clock. */
    @Order(5)
    long reqSndTs;

    /** Request serialization-start timestamp from the source node wall clock. */
    @Order(6)
    long reqSndTsMillis;

    /** Request deserialization-completion timestamp from the target node monotonic clock. */
    @Order(7)
    long reqRcvTs;

    /** Request deserialization-completion timestamp from the target node wall clock. */
    @Order(8)
    long reqRcvTsMillis;

    /** Request listener invocation timestamp from the target node monotonic clock. */
    @Order(9)
    long reqProcTs;

    /** Response serialization-start timestamp from the target node monotonic clock. */
    @Order(10)
    long resSndTs;

    /** Response serialization-start timestamp from the target node wall clock. */
    @Order(11)
    long resSndTsMillis;

    /** Response deserialization-completion timestamp from the source node monotonic clock. */
    long resRcvTs;

    /** Response deserialization-completion timestamp from the source node wall clock. */
    long resRcvTsMillis;

    /** Response listener invocation timestamp from the source node monotonic clock. */
    long resProcTs;

    /** Required by the message factory. */
    public IgniteIoTestMessage() {
        // No-op.
    }

    /** Request constructor. */
    public IgniteIoTestMessage(long id, byte[] payload, boolean procFromNioThread) {
        this.id = id;
        this.payload = payload;
        this.procFromNioThread = procFromNioThread;

        req = true;
        reqCreateTs = System.nanoTime();
    }

    /** Response constructor. */
    public IgniteIoTestMessage(IgniteIoTestMessage req) {
        id = req.id;
        payload = req.payload;
        procFromNioThread = req.procFromNioThread;
        reqCreateTs = req.reqCreateTs;
        reqSndTs = req.reqSndTs;
        reqSndTsMillis = req.reqSndTsMillis;
        reqRcvTs = req.reqRcvTs;
        reqRcvTsMillis = req.reqRcvTsMillis;
        reqProcTs = req.reqProcTs;
    }

    /** @return {@code True} to process this message in NIO thread. */
    public boolean processFromNioThread() {
        return procFromNioThread;
    }

    /** @return {@code True} if this is a request. */
    public boolean request() {
        return req;
    }

    /** @return Test ID. */
    public long testId() {
        return id;
    }

    /** Records request listener invocation. */
    void onRequestProcessed() {
        reqProcTs = System.nanoTime();
    }

    /** Records response listener invocation. */
    void onResponseProcessed() {
        resProcTs = System.nanoTime();
    }

    /** @return End-to-end RTT from request creation to response listener invocation, in nanoseconds. */
    long roundTripNanos() {
        return resProcTs - reqCreateTs;
    }

    /** @return Delay from request creation to request serialization start, in nanoseconds. */
    long requestSendQueueNanos() {
        return reqSndTs - reqCreateTs;
    }

    /** @return Delay from request deserialization completion to request listener invocation, in nanoseconds. */
    long requestReceiveQueueNanos() {
        return reqProcTs - reqRcvTs;
    }

    /** @return Delay from request listener invocation to response serialization start, in nanoseconds. */
    long responseSendQueueNanos() {
        return resSndTs - reqProcTs;
    }

    /** @return Delay from response deserialization completion to response listener invocation, in nanoseconds. */
    long responseReceiveQueueNanos() {
        return resProcTs - resRcvTs;
    }

    /**
     * @return Estimated one-way request delay from serialization start on the source to deserialization completion on
     *     the target, in milliseconds. Requires synchronized wall clocks.
     */
    long requestWireTimeMillis() {
        return reqRcvTsMillis - reqSndTsMillis;
    }

    /**
     * @return Estimated one-way response delay from serialization start on the target to deserialization completion on
     *     the source, in milliseconds. Requires synchronized wall clocks.
     */
    long responseWireTimeMillis() {
        return resRcvTsMillis - resSndTsMillis;
    }

    /** Records the start of the first serialization attempt. */
    void onBeforeWrite() {
        if (req && reqSndTs == 0) {
            reqSndTs = System.nanoTime();
            reqSndTsMillis = System.currentTimeMillis();
        }
        else if (!req && resSndTs == 0) {
            resSndTs = System.nanoTime();
            resSndTsMillis = System.currentTimeMillis();
        }
    }

    /** Records the completion of deserialization. */
    void onAfterRead() {
        if (req && reqRcvTs == 0) {
            reqRcvTs = System.nanoTime();
            reqRcvTsMillis = System.currentTimeMillis();
        }
        else if (!req && resRcvTs == 0) {
            resRcvTs = System.nanoTime();
            resRcvTsMillis = System.currentTimeMillis();
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        onBeforeWrite();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        onAfterRead();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIoTestMessage.class, this);
    }
}
