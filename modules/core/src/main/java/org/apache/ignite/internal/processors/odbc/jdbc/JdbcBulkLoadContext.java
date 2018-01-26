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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.internal.processors.bulkload.BulkLoadContext;

/** A wrapper class for a core {@link BulkLoadContext}, which adds JDBC-specific state. */
public class JdbcBulkLoadContext {

    /** Result of a parsing the COPY command to send to the client. */
    private final JdbcBulkLoadBatchRequestResult cmdParsingResult;

    /** Core bulk load context. */
    private final BulkLoadContext loadContext;

    /** Updates counter. */
    private int updateCnt;

    /**
     * Creates the JDBC bulk load context.
     *
     * @param cmdParsingResult Result of a parsing the COPY command to send to the client.
     * @param context Core bulk load context.
     */
    public JdbcBulkLoadContext(JdbcBulkLoadBatchRequestResult cmdParsingResult, BulkLoadContext context) {
        this.cmdParsingResult = cmdParsingResult;
        loadContext = context;
        updateCnt = 0;
    }

    /**
     * Returns the query ID.
     *
     * @return query ID.
     */
    public long queryId() {
        return cmdParsingResult.queryId();
    }

    /**
     * Returns the result of a parsing the COPY command to send to the client.
     *
     * @return esult of a parsing the COPY command to send to the client.
     */
    public JdbcBulkLoadBatchRequestResult cmdParsingResult() {
        return cmdParsingResult;
    }

    /**
     * Increments update counter by the argument.
     *
     * @param incr The value to increment the update counter by.
     */
    public void incrementUpdateCountBy(int incr) {
        updateCnt += incr;
    }

    /**
     * Returns the update counter.
     *
     * @return The update counter.
     */
    public int updateCnt() {
        return updateCnt;
    }

    /**
     * Returns the core bulk load context.
     *
     * @return The core bulk load context.
     */
    public BulkLoadContext loadContext() {
        return loadContext;
    }

    /**
     * Closes the context and frees the resources.
     */
    public void close() {
        loadContext.close();
    }
}
