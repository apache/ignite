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

import java.nio.charset.Charset;

/** Bulk load parameters -- which was specified by the user or their default values. */
public class BulkLoadParameters {

    /** Minimal batch size. */
    public static final int MIN_BATCH_SIZE = 1;

    /** Maximal batch size. Note that the batch is wrapped to transport objects and the overall packet should fit
     * into a Java array.
     */
    public static final int MAX_BATCH_SIZE = Integer.MAX_VALUE - 512;

    /** Size of a file batch for COPY command. */
    public static final int DEFAULT_BATCH_SIZE = 4 * 1024 * 1024;

    /** The default input charset. */
    public static final Charset DEFAULT_INPUT_CHARSET = Charset.forName("UTF-8");

    /** Local name of the file to send to server */
    @NotNull private final String locFileName;

    /** File batch size in bytes. */
    private final int batchSize;

    /**
     * Creates a bulk load parameters.
     * @param locFileName File name to send from client to server.
     * @param batchSize Batch size (Number of bytes in a portion of a file to send in one Jdbc request/response).
     */
    public BulkLoadParameters(String locFileName, int batchSize) {
        this.locFileName = locFileName;
        this.batchSize = batchSize;
    }

    /**
     * Returns the local name of file to send.
     *
     * @return locFileName the local name of file to send.
     */
    public String localFileName() {
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
}
