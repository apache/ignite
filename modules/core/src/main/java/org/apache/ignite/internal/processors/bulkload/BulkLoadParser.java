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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadContext;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest;

import java.util.List;

/**
 * Bulk load file format parser superclass + factory.
 *
 * <p>The parser uses corresponding options (see {@link BulkLoadFormat} class).
 *
 * Parser processes a batch of input data and return a list of records.
 */
public abstract class BulkLoadParser {

    /** Format options. */
    private final BulkLoadFormat formatOpts;

    /** Creates a parser with given options.
     *
     * @param formatOpts The format options.
     */
    protected BulkLoadParser(BulkLoadFormat formatOpts) {
        this.formatOpts = formatOpts;
    }

    /**
     * Processes a batch of input data. Context and request are supplied as parameters.
     * Returns a list of records parsed (in most cases this is a list of strings).
     *
     * <p>Note that conversion between parsed and database table type is done by the other
     * object (a subclass of {@link BulkLoadEntryConverter)) by the request processing code.
     * This method is not obliged to do this conversion.
     *
     * @param ctx Context of the bulk load operation.
     * @param req The current request.
     * @return The list of records.
     * @throws IgniteCheckedException If any processing error occurs.
     */
    public abstract Iterable<List<Object>> processBatch(JdbcBulkLoadContext ctx, JdbcBulkLoadBatchRequest req) throws IgniteCheckedException;

    /**
     * Creates a parser for a given format options.
     *
     * @param format The input format object.
     * @return The parser.
     * @throws IllegalArgumentException if the format is not known to the factory.
     */
    public static BulkLoadParser createParser(BulkLoadFormat format) {
        if (format instanceof BulkLoadCsvFormat)
            return new BulkLoadCsvParser(format);

        throw new IllegalArgumentException("Internal error: format is not defined");
    }
}
