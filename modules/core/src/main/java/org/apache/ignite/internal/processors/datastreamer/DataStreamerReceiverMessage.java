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

/** DataStreamer cache receiver/updater message. */
@UseBinaryMarshaller
public class DataStreamerReceiverMessage implements Message {
    /** DataStreamer cache receiver/updater; {@code null} when {@link #builtIn} names it. */
    @Marshalled("rcvrBytes")
    StreamReceiver<?, ?> rcvr;

    /** Serialized {@link #rcvr}. */
    @Order(0)
    volatile byte[] rcvrBytes;

    /** The updater every node has; {@code null} when {@link #rcvr} is a user one. */
    @Order(1)
    DataStreamerBuiltInUpdater builtIn;

    /** Empty constructor for serialization purposes. */
    public DataStreamerReceiverMessage() {
        // No-op.
    }

    /** @param rcvr User receiver. */
    DataStreamerReceiverMessage(StreamReceiver<?, ?> rcvr) {
        assert DataStreamerBuiltInUpdater.of(rcvr) == null : "A built-in updater travels by name: " + rcvr;

        this.rcvr = rcvr;
    }

    /** @param builtIn Updater every node has, named rather than carried. */
    DataStreamerReceiverMessage(DataStreamerBuiltInUpdater builtIn) {
        this.builtIn = builtIn;
    }

    /** @return {@code True} if the receiver travels with this message rather than being named. */
    boolean user() {
        return builtIn == null;
    }

    /** @return Receiver: the one carried here, or the node's own that this message names. */
    StreamReceiver<?, ?> receiver() {
        return builtIn == null ? rcvr : builtIn.updater();
    }
}
