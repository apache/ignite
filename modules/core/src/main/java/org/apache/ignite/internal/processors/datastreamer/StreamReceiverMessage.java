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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.stream.StreamReceiver;

/**
 * The receiver of a streamer on its way to the nodes that own the data: a user object here, its serialized form on
 * the wire. One instance serves every batch of a streamer, so the receiver is marshalled once and the batches share
 * the result; a streamer given another receiver builds another instance.
 */
@UseBinaryMarshaller
public class StreamReceiverMessage implements Message {
    /** */
    @Marshalled("rcvrBytes")
    StreamReceiver<?, ?> rcvr;

    /**
     * Serialized {@link #rcvr}, written by whichever batch is marshalled first and read by the rest. Those batches
     * leave on different threads, hence the {@code volatile}: a reader seeing the reference before the contents would
     * skip the marshalling and send a half-written array.
     */
    @Order(0)
    volatile byte[] rcvrBytes;

    /** Empty constructor. */
    public StreamReceiverMessage() {
        // No-op.
    }

    /** @param rcvr Receiver. */
    StreamReceiverMessage(StreamReceiver<?, ?> rcvr) {
        this.rcvr = rcvr;
    }

    /** @return Receiver. */
    StreamReceiver<?, ?> receiver() {
        return rcvr;
    }
}
