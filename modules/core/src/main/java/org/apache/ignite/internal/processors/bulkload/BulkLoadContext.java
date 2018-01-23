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

    private final BulkLoadParameters params;

    private final BulkLoadParser inputParser;

    private final BulkLoadEntryConverter dataConverter;

    private final IgniteDataStreamer<Object, Object> outputStreamer;

    public BulkLoadContext(BulkLoadParameters params, BulkLoadParser inputParser, BulkLoadEntryConverter dataConverter,
        IgniteDataStreamer<Object, Object> outputStreamer) {

        this.params = params;
        this.inputParser = inputParser;
        this.dataConverter = dataConverter;
        this.outputStreamer = outputStreamer;
    }

    /**
     * Returns the params.
     *
     * @return params.
     */
    public BulkLoadParameters params() {
        return params;
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
     * Returns the dataConverter.
     *
     * @return dataConverter.
     */
    public BulkLoadEntryConverter dataConverter() {
        return dataConverter;
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
