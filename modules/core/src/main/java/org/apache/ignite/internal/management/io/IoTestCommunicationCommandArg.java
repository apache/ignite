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

package org.apache.ignite.internal.management.io;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;

/** */
public class IoTestCommunicationCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Argument(description = "Source node ID")
    UUID nodeId;

    /** */
    @Order(1)
    @Argument(optional = true, description = "Warmup duration (millis, max 1 hour, 5000 by default)")
    long warmup = TimeUnit.SECONDS.toMillis(5);

    /** */
    @Order(2)
    @Argument(optional = true, description = "Test duration (millis, max 1 hour, 30000 by default)")
    long duration = TimeUnit.SECONDS.toMillis(30);

    /** */
    @Order(3)
    @Argument(optional = true, description = "Number of test threads (max 64, 1 by default)")
    int threads = 1;

    /** */
    @Order(4)
    @Argument(optional = true, description = "Payload size in each direction (bytes, max 1 MiB, 0 by default)")
    int payloadSize;

    /** */
    @Order(5)
    @Argument(optional = true, description = "Process requests and responses in NIO threads")
    boolean processInNioThread;

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public long warmup() {
        return warmup;
    }

    /** */
    public void warmup(long warmup) {
        this.warmup = warmup;
    }

    /** */
    public long duration() {
        return duration;
    }

    /** */
    public void duration(long duration) {
        this.duration = duration;
    }

    /** */
    public int threads() {
        return threads;
    }

    /** */
    public void threads(int threads) {
        this.threads = threads;
    }

    /** */
    public int payloadSize() {
        return payloadSize;
    }

    /** */
    public void payloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    /** */
    public boolean processInNioThread() {
        return processInNioThread;
    }

    /** */
    public void processInNioThread(boolean processInNioThread) {
        this.processInNioThread = processInNioThread;
    }
}
