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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.sql.GridCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.h2.command.Prepared;

/**
 *
 */
class DdlStatementsProcessor {
    /** */
    private final IgniteH2Indexing indexing;

    /** */
    DdlStatementsProcessor(IgniteH2Indexing indexing) {
        this.indexing = indexing;
    }

    /** */
    QueryCursor<List<?>> runDdlStatement(GridCacheContext<?, ?> cctx, Prepared stmt) throws IgniteCheckedException {
        GridSqlStatement gridStmt = new GridSqlQueryParser().parse(stmt);

        if (gridStmt instanceof GridCreateIndex) {
            QueryIndex newIdx = ((GridCreateIndex) gridStmt).toQueryIndex();

            throw new UnsupportedOperationException("CREATE INDEX");
        }
        else if (gridStmt instanceof GridDropIndex)
            throw new UnsupportedOperationException("DROP INDEX");
        else
            throw new IgniteSQLException("Unexpected DDL operation [type=" + gridStmt.getClass() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);
    }
}
