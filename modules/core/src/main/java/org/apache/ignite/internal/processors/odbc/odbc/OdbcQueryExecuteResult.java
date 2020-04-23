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
    private final long[] affectedRows;

    /**
     * @param queryId Query ID.
     * @param columnsMetadata Columns metadata.
     * @param affectedRows Affected rows.
     */
    public OdbcQueryExecuteResult(long queryId, Collection<OdbcColumnMeta> columnsMetadata, long[] affectedRows) {
        this.queryId = queryId;
        this.columnsMetadata = columnsMetadata;
        this.affectedRows = affectedRows;
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
    public long[] affectedRows() {
        return affectedRows;
    }
}
