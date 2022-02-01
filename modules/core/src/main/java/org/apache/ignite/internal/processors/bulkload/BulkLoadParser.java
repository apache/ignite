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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;

/**
 * Bulk load file format parser superclass + factory of known formats.
 *
 * <p>The parser processes a batch of input data and return a list of records.
 *
 * <p>The parser uses corresponding options from {@link BulkLoadFormat} subclass.
 */
public abstract class BulkLoadParser {
    /**
     * Parses a batch of input data and returns a list of records parsed
     * (in most cases this is a list of strings).
     *
     * <p>Note that conversion between parsed and database table type is done by the other
     * object (see {@link BulkLoadProcessor#dataConverter}) by the request processing code.
     * This method is not obliged to do this conversion.
     *
     * @param batchData Data from the current batch.
     * @param isLastBatch true if this is the last batch.
     * @return The list of records.
     * @throws IgniteCheckedException If any processing error occurs.
     */
    protected abstract Iterable<List<Object>> parseBatch(byte[] batchData, boolean isLastBatch)
        throws IgniteCheckedException;

    /**
     * Creates a parser for a given format options.
     *
     * @param format The input format object.
     * @return The parser.
     * @throws IllegalArgumentException if the format is not known to the factory.
     */
    public static BulkLoadParser createParser(BulkLoadFormat format) {
        if (format instanceof BulkLoadCsvFormat)
            return new BulkLoadCsvParser((BulkLoadCsvFormat)format);

        throw new IllegalArgumentException("Internal error: format is not defined");
    }
}
