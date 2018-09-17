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

package org.apache.ignite.internal.processors.odbc.odbc;

import java.util.Collection;

/**
 * SQL listener query execute result.
 */
public class OdbcQueryExecuteResult {
    /** Query ID. */
    private final long queryId;

    /** Fields metadata. */
    private final Collection<OdbcColumnMeta> columnsMetadata;

    /** Rows affected by the statements. */
    private final Collection<Long> affectedRows;

    /** Indicates whether result set was closed on server side. */
    private final boolean closed;

    /**
     * @param queryId Query ID.
     * @param columnsMetadata Columns metadata.
     * @param affectedRows Affected rows.
     * @param closed Closed result set indicator.
     */
    public OdbcQueryExecuteResult(long queryId, Collection<OdbcColumnMeta> columnsMetadata,
        Collection<Long> affectedRows, boolean closed) {
        this.queryId = queryId;
        this.columnsMetadata = columnsMetadata;
        this.affectedRows = affectedRows;
        this.closed = closed;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * @return Columns metadata.
     */
    public Collection<OdbcColumnMeta> columnsMetadata() {
        return columnsMetadata;
    }

    /**
     * @return Number of rows affected by the statements.
     */
    public Collection<Long> affectedRows() {
        return affectedRows;
    }

    /**
     * @return {@code true} if result set was closed on server side.
     */
    public boolean closed() {
        return closed;
    }
}
