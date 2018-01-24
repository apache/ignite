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

/** FIXME SHQ */
public class JdbcBulkLoadContext {

    private final JdbcBulkLoadBatchRequestResult cmdParsingResult;

    private final BulkLoadContext loadContext;

    private int updateCnt;

    public JdbcBulkLoadContext(JdbcBulkLoadBatchRequestResult cmdParsingResult, BulkLoadContext context) {
        this.cmdParsingResult = cmdParsingResult;
        loadContext = context;
        updateCnt = 0;
    }

    /**
     * Returns the queryId.
     *
     * @return queryId.
     */
    public long queryId() {
        return cmdParsingResult.queryId();
    }

    /**
     * Returns the cmdParsingResult.
     *
     * @return cmdParsingResult.
     */
    public JdbcBulkLoadBatchRequestResult cmdParsingResult() {
        return cmdParsingResult;
    }

    public void incrementUpdateCountBy(int incr) {
        updateCnt += incr;
    }

    /**
     * Returns the updateCnt.
     *
     * @return updateCnt.
     */
    public int updateCnt() {
        return updateCnt;
    }

    /**
     * Returns the loadContext.
     *
     * @return loadContext.
     */
    public BulkLoadContext loadContext() {
        return loadContext;
    }

    public void close() {
        loadContext.close();
    }
}
