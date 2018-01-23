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

import org.apache.ignite.IgniteDataStreamer;

/** FIXME SHQ */
public class BulkLoadContext {

    /** Local name of the file to send to server */
    private String locFileName;

    private final BulkLoadParser inputParser;

    private final BulkLoadEntryConverter updatePlan;

    private final IgniteDataStreamer<Object, Object> outputStreamer;

    public BulkLoadContext(String locFileName, BulkLoadParser inputParser, BulkLoadEntryConverter updatePlan,
        IgniteDataStreamer<Object, Object> outputStreamer) {

        this.locFileName = locFileName;
        this.inputParser = inputParser;
        this.updatePlan = updatePlan;
        this.outputStreamer = outputStreamer;
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
     * Returns the inputParser.
     *
     * @return inputParser.
     */
    public BulkLoadParser inputParser() {
        return inputParser;
    }

    /**
     * Returns the updatePlan.
     *
     * @return updatePlan.
     */
    public BulkLoadEntryConverter dataConverter() {
        return updatePlan;
    }

    /**
     * Returns the outputStreamer.
     *
     * @return outputStreamer.
     */
    public IgniteDataStreamer<Object, Object> outputStreamer() {
        return outputStreamer;
    }

    /**
     * Closes the underlyinng objects ({@link IgniteDataStreamer}).
     * */
    public void close() {
        outputStreamer.close();
    }
}
