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

package org.apache.ignite.internal.tx;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;

/**
 * A timestamp implementation. Timestamps are used to order transactions and perform conflict resolution.
 *
 * <p>The timestamp has the following structure:
 *
 * <p>Epoch time(48 bit), Local counter (16 bit), Local node id (48 bits), Reserved (16 bits)
 */
public class Timestamp implements Comparable<Timestamp>, Serializable {
    /** Serial version. */
    private static final long serialVersionUID = 1L;

    /** Epoch start for the generation purposes. */
    private static final long EPOCH = LocalDateTime.of(2021, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

    /** A max value for a counter before rollover. */
    public static final short MAX_CNT = Short.MAX_VALUE;

    /** Local time. */
    private static long localTime;

    /** The counter. */
    private static long cntr;

    /** Local node id. */
    private static long localNodeId = getLocalNodeId();

    /** The offset and counter part of a timestamp. */
    private final long timestamp;

    /** The node id part of a timestamp. */
    private final long nodeId;

    /**
     * The constructor.
     *
     * @param timestamp The timestamp.
     * @param nodeId    Node id.
     */
    public Timestamp(long timestamp, long nodeId) {
        this.timestamp = timestamp;
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(@NotNull Timestamp other) {
        return (this.timestamp < other.timestamp ? -1 : (this.timestamp > other.timestamp ? 1 :
                (this.nodeId < other.nodeId ? -1 : (this.nodeId > other.nodeId ? 1 : 0))));
    }

    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Return a node id.
     *
     * @return Local node id.
     */
    public long getNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Timestamp)) {
            return false;
        }

        return compareTo((Timestamp) o) == 0;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        long hilo = timestamp ^ nodeId;
        return ((int) (hilo >> 32)) ^ (int) hilo;
    }

    /**
     * Generates new monotonically increasing timestamp.
     * TODO https://issues.apache.org/jira/browse/IGNITE-15129
     *
     * @return Next timestamp (monotonically increasing).
     */
    public static synchronized Timestamp nextVersion() {
        long timestamp = Clock.systemUTC().instant().toEpochMilli() - EPOCH;

        long newTime = Math.max(localTime, timestamp);

        if (newTime == localTime) {
            cntr = (cntr + 1) & 0xFFFF;

            if (cntr == 0) {
                newTime = waitForNextMillisecond(newTime);
            }
        } else {
            cntr = 0;
        }

        localTime = newTime;

        // Will overflow in a late future.
        return new Timestamp(newTime << 16 | cntr, localNodeId);
    }

    /**
     * Waits until a time will pass after a timestamp.
     *
     * @param lastTimestamp The timestamp.
     * @return New timestamp.
     */
    private static long waitForNextMillisecond(long lastTimestamp) {
        long timestamp;

        do {
            timestamp = Clock.systemUTC().instant().toEpochMilli() - EPOCH;
        } while (timestamp <= lastTimestamp); // Wall clock can go backward.

        return timestamp;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new UUID(timestamp, nodeId).toString();
    }

    /**
     * Generates a local node id.
     *
     * @return Local node id as a long value.
     */
    private static long getLocalNodeId() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();

            NetworkInterface iface = NetworkInterface.getByInetAddress(localHost);

            byte[] bytes = null;

            if (iface != null) {
                bytes = iface.getHardwareAddress();
            }

            if (bytes == null) {
                ThreadLocalRandom.current().nextBytes(bytes = new byte[Byte.SIZE]);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Byte.SIZE);
            buffer.put(bytes);

            for (int i = bytes.length; i < Byte.SIZE; i++) {
                buffer.put((byte) 0);
            }

            buffer.flip();

            return buffer.getLong();
        } catch (Exception e) {
            throw new IgniteException("Failed to get local node id", e);
        }
    }
}
