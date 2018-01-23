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

/** FIXME SHQ */
public class BulkLoadParameters {

    public static final int MIN_BATCH_SIZE = 1;

    public static final int MAX_BATCH_SIZE = Integer.MAX_VALUE - 512;

    /** Size of a file batch for COPY command. */
    public static final int DEFAULT_BATCH_SIZE = 4 * 1024 * 1024;

    /** Local name of the file to send to server */
    private String locFileName;

    /** File batch size in bytes. */
    private int batchSize;

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
     * Returns the batchSize.
     *
     * @return batchSize.
     */
    public int batchSize() {
        return batchSize;
    }
}
