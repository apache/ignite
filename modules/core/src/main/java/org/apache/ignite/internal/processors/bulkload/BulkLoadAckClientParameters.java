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
    /** Minimum batch size. */
    public static final int MIN_BATCH_SIZE = 1;

    /**
     * Maximum batch size. Note that the batch is wrapped to transport objects and the overall packet should fit
     * into a Java array. 512 has been chosen arbitrarily.
     */
    public static final int MAX_BATCH_SIZE = Integer.MAX_VALUE - 512;

    /** Size of a file batch for COPY command. */
    public static final int DEFAULT_BATCH_SIZE = 4 * 1024 * 1024;

    /** Local name of the file to send to server */
    @NotNull private final String locFileName;

    /** File batch size in bytes. */
    private final int batchSize;

    /**
     * Creates a bulk load parameters.
     *
     * @param locFileName File name to send from client to server.
     * @param batchSize Batch size (Number of bytes in a portion of a file to send in one JDBC request/response).
     */
    public BulkLoadAckClientParameters(@NotNull String locFileName, int batchSize) {
        this.locFileName = locFileName;
        this.batchSize = batchSize;
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
     * Returns the batch size.
     *
     * @return The batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Checks if batch size value is valid.
     *
     * @param sz The batch size to check.
     * @throws IllegalArgumentException if batch size is invalid.
     */
    public static boolean isValidBatchSize(int sz) {
        return sz >= MIN_BATCH_SIZE && sz <= MAX_BATCH_SIZE;
    }

    /**
     * Creates proper batch size error message if {@link #isValidBatchSize(int)} check has failed.
     *
     * @param sz The batch size.
     * @return The string with the error message.
     */
    public static String batchSizeErrorMsg(int sz) {
        return "Batch size should be within [" + MIN_BATCH_SIZE + ".." + MAX_BATCH_SIZE + "]: " + sz;
    }
}
