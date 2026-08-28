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
    boolean processInNioThread;

    /** Request flag. */
    @Order(2)
    boolean req;

    /** Payload. */
    @Order(3)
    byte[] payload;

    /** Request pre-send marshalling timestamp from the source node wall clock. */
    @Order(4)
    long reqSndTsMillis;

    /** Request pre-listener unmarshalling timestamp from the target node wall clock. */
    @Order(5)
    long reqRcvTsMillis;

    /** Response pre-send marshalling timestamp from the target node wall clock. */
    @Order(6)
    long resSndTsMillis;

    /** Response pre-listener unmarshalling timestamp from the source node wall clock. */
    @Order(7)
    long resRcvTsMillis;

    /** End-to-end RTT measured by the source node. */
    private long rttNanos;

    /** Required by the message factory. */
    public IgniteIoTestMessage() {
        // No-op.
    }

    /** Request constructor. */
    public IgniteIoTestMessage(long id, byte[] payload, boolean processInNioThread) {
        this.id = id;
        this.payload = payload;
        this.processInNioThread = processInNioThread;

        req = true;
    }

    /** Response constructor. */
    public IgniteIoTestMessage(IgniteIoTestMessage req) {
        id = req.id;
        payload = req.payload;
        processInNioThread = req.processInNioThread;
        reqSndTsMillis = req.reqSndTsMillis;
        reqRcvTsMillis = req.reqRcvTsMillis;
    }

    /** @return {@code True} to process this message in NIO thread. */
    public boolean processInNioThread() {
        return processInNioThread;
    }

    /** @return {@code True} if this is a request. */
    public boolean request() {
        return req;
    }

    /** @return Test ID. */
    public long testId() {
        return id;
    }

    /** Sets end-to-end RTT measured by the source node. */
    void roundTripNanos(long rttNanos) {
        this.rttNanos = rttNanos;
    }

    /** @return End-to-end RTT in nanoseconds. */
    long roundTripNanos() {
        return rttNanos;
    }

    /**
     * @return Estimated one-way request delivery delay from pre-send marshalling on the source to pre-listener
     *     unmarshalling on the target, in milliseconds. Requires synchronized wall clocks.
     */
    long requestDeliveryTimeMillis() {
        return reqRcvTsMillis - reqSndTsMillis;
    }

    /**
     * @return Estimated one-way response delivery delay from pre-send marshalling on the target to pre-listener
     *     unmarshalling on the source, in milliseconds. Requires synchronized wall clocks.
     */
    long responseDeliveryTimeMillis() {
        return resRcvTsMillis - resSndTsMillis;
    }

    /** Records the first pre-send marshalling of this request or response. */
    @Override public void marshal(Marshaller marsh) {
        recordSendTimestamp();
    }

    /** Records the synthetic pre-send hook for local delivery, which bypasses marshalling. */
    void onBeforeLocalSend() {
        recordSendTimestamp();
    }

    /** Records the first pre-send timestamp. */
    private void recordSendTimestamp() {
        if (req && reqSndTsMillis == 0)
            reqSndTsMillis = System.currentTimeMillis();
        else if (!req && resSndTsMillis == 0)
            resSndTsMillis = System.currentTimeMillis();
    }

    /** Records pre-listener unmarshalling of this request or response. */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (req && reqRcvTsMillis == 0)
            reqRcvTsMillis = System.currentTimeMillis();
        else if (!req && resRcvTsMillis == 0)
            resRcvTsMillis = System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIoTestMessage.class, this);
    }
}
