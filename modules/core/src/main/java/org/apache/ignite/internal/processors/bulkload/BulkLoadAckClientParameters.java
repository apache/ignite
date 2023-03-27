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

package org.apache.ignite.internal.processors.bulkload;

import org.jetbrains.annotations.NotNull;

/**
 * Bulk load parameters, which are parsed from SQL command and sent from server to client.
 */
public class BulkLoadAckClientParameters {
    /** Minimum packet size. */
    public static final int MIN_PACKET_SIZE = 1;

    /**
     * Maximum packet size. Note that the packet is wrapped to transport objects and the overall packet should fit
     * into a Java array. 512 has been chosen arbitrarily.
     */
    public static final int MAX_PACKET_SIZE = Integer.MAX_VALUE - 512;

    /** Size of a file packet size for COPY command. */
    public static final int DFLT_PACKET_SIZE = 4 * 1024 * 1024;

    /** Local name of the file to send to server */
    @NotNull private final String locFileName;

    /** File packet size in bytes. */
    private final int packetSize;

    /**
     * Creates a bulk load parameters.
     *
     * @param locFileName File name to send from client to server.
     * @param packetSize Packet size (Number of bytes in a portion of a file to send in one JDBC request/response).
     */
    public BulkLoadAckClientParameters(@NotNull String locFileName, int packetSize) {
        this.locFileName = locFileName;
        this.packetSize = packetSize;
    }

    /**
     * Returns the local name of file to send.
     *
     * @return The local name of file to send.
     */
    @NotNull public String localFileName() {
        return locFileName;
    }

    /**
     * Returns the packet size.
     *
     * @return The packet size.
     */
    public int packetSize() {
        return packetSize;
    }

    /**
     * Checks if packet size value is valid.
     *
     * @param sz The packet size to check.
     * @throws IllegalArgumentException if packet size is invalid.
     */
    public static boolean isValidPacketSize(int sz) {
        return sz >= MIN_PACKET_SIZE && sz <= MAX_PACKET_SIZE;
    }

    /**
     * Creates proper packet size error message if {@link #isValidPacketSize(int)} check has failed.
     *
     * @param size The packet size.
     * @return The string with the error message.
     */
    public static String packetSizeErrorMesssage(int size) {
        return "Packet size should be within [" + MIN_PACKET_SIZE + ".." + MAX_PACKET_SIZE + "]: " + size;
    }
}
