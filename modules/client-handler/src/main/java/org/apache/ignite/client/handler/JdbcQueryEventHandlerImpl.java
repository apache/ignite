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

package org.apache.ignite.client.handler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.handler.requests.sql.JdbcMetadataCatalog;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.event.BatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.BatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcColumnMeta;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesResult;
import org.apache.ignite.client.proto.query.event.JdbcPrimaryKeyMeta;
import org.apache.ignite.client.proto.query.event.JdbcTableMeta;
import org.apache.ignite.client.proto.query.event.QueryCloseRequest;
import org.apache.ignite.client.proto.query.event.QueryCloseResult;
import org.apache.ignite.client.proto.query.event.QueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.QueryExecuteResult;
import org.apache.ignite.client.proto.query.event.QueryFetchRequest;
import org.apache.ignite.client.proto.query.event.QueryFetchResult;
import org.apache.ignite.client.proto.query.event.QuerySingleResult;
import org.apache.ignite.client.proto.query.event.Response;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.util.Cursor;

import static org.apache.ignite.client.proto.query.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl implements JdbcQueryEventHandler {
    /** Current JDBC cursors. */
    private final ConcurrentHashMap<Long, SqlCursor<List<?>>> openCursors = new ConcurrentHashMap<>();

    /** Cursor Id generator. */
    private final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Jdbc metadata info. */
    private final JdbcMetadataCatalog meta;

    /**
     * Constructor.
     *
     * @param processor Processor.
     * @param meta JdbcMetadataInfo.
     */
    public JdbcQueryEventHandlerImpl(QueryProcessor processor, JdbcMetadataCatalog meta) {
        this.processor = processor;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override public QueryExecuteResult query(QueryExecuteRequest req) {
        if (req.pageSize() <= 0)
            return new QueryExecuteResult(Response.STATUS_FAILED,
                "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

        List<SqlCursor<List<?>>> cursors;
        try {
            cursors = processor.query(req.schemaName(), req.sqlQuery(), req.arguments() == null ? new Object[0] : req.arguments());
        } catch (Exception e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return new QueryExecuteResult(Response.STATUS_FAILED,
                "Exception while executing query " + req.sqlQuery() + ". Error message: " + sw);
        }

        if (cursors.isEmpty())
            return new QueryExecuteResult(Response.STATUS_FAILED,
                "At least one cursor is expected for query " + req.sqlQuery());

        List<QuerySingleResult> results = new ArrayList<>();

        try {
            for (SqlCursor<List<?>> cur : cursors) {
                QuerySingleResult res = createJdbcResult(cur, req);
                results.add(res);
            }
        } catch (Exception ex) {
            StringWriter sw = getWriterWithStackTrace(ex);

            return new QueryExecuteResult(Response.STATUS_FAILED,
                "Failed to fetch results for query " + req.sqlQuery() + ". Error message: " + sw);
        }

        return new QueryExecuteResult(results);
    }

    /** {@inheritDoc} */
    @Override public QueryFetchResult fetch(QueryFetchRequest req) {
        Cursor<List<?>> cur = openCursors.get(req.cursorId());

        if (cur == null)
            return new QueryFetchResult(Response.STATUS_FAILED,
                "Failed to find query cursor with ID: " + req.cursorId());

        if (req.pageSize() <= 0)
            return new QueryFetchResult(Response.STATUS_FAILED,
                "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

        List<List<Object>> fetch;
        boolean hasNext;

        try {
            fetch = fetchNext(req.pageSize(), cur);
            hasNext = cur.hasNext();
        } catch (Exception ex) {
            StringWriter sw = getWriterWithStackTrace(ex);

            return new QueryFetchResult(Response.STATUS_FAILED,
                "Failed to fetch results for cursor id " + req.cursorId() + ". Error message: " + sw);
        }

        return new QueryFetchResult(fetch, hasNext);
    }

    /** {@inheritDoc} */
    @Override public BatchExecuteResult batch(BatchExecuteRequest req) {
        return new BatchExecuteResult(UNSUPPORTED_OPERATION,
            "ExecuteBatch operation is not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public QueryCloseResult close(QueryCloseRequest req) {
        Cursor<List<?>> cur = openCursors.remove(req.cursorId());

        if (cur == null)
            return new QueryCloseResult(Response.STATUS_FAILED,
                "Failed to find query cursor with ID: " + req.cursorId());

        try {
            cur.close();
        }
        catch (Exception ex) {
            StringWriter sw = getWriterWithStackTrace(ex);

            return new QueryCloseResult(Response.STATUS_FAILED,
                "Failed to close SQL query [curId=" + req.cursorId() + "]. Error message: " + sw);
        }

        return new QueryCloseResult();
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaTablesResult tablesMeta(JdbcMetaTablesRequest req) {
        List<JdbcTableMeta> tblsMeta = meta.getTablesMeta(req.schemaName(), req.tableName(), req.tableTypes());

        return new JdbcMetaTablesResult(tblsMeta);
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaColumnsResult columnsMeta(JdbcMetaColumnsRequest req) {
        Collection<JdbcColumnMeta> tblsMeta = meta.getColumnsMeta(req.schemaName(), req.tableName(), req.columnName());

        return new JdbcMetaColumnsResult(tblsMeta);
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaSchemasResult schemasMeta(JdbcMetaSchemasRequest req) {
        Collection<String> tblsMeta = meta.getSchemasMeta(req.schemaName());

        return new JdbcMetaSchemasResult(tblsMeta);
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaPrimaryKeysResult primaryKeysMeta(JdbcMetaPrimaryKeysRequest req) {
        Collection<JdbcPrimaryKeyMeta> tblsMeta = meta.getPrimaryKeys(req.schemaName(), req.tableName());

        return new JdbcMetaPrimaryKeysResult(tblsMeta);
    }

    /**
     * Serializes the stack trace of given exception for further sending to the client.
     *
     * @param ex Exception.
     * @return StringWriter filled with exception.
     */
    private StringWriter getWriterWithStackTrace(Exception ex) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        ex.printStackTrace(pw);
        return sw;
    }

    /**
     * Creates jdbc result for the cursor.
     *
     * @param cur Sql cursor for query.
     * @param req Execution request.
     * @return JdbcQuerySingleResult filled with first batch of data.
     */
    private QuerySingleResult createJdbcResult(SqlCursor<List<?>> cur, QueryExecuteRequest req) {
        long cursorId = CURSOR_ID_GENERATOR.getAndIncrement();

        openCursors.put(cursorId, cur);

        List<List<Object>> fetch = fetchNext(req.pageSize(), cur);
        boolean hasNext = cur.hasNext();

        switch (cur.getQueryType()) {
            case EXPLAIN:
            case QUERY:
                return new QuerySingleResult(cursorId, fetch, !hasNext);
            case DML:
            case DDL: {
                if (!validateDmlResult(fetch, hasNext))
                    return new QuerySingleResult(Response.STATUS_FAILED,
                        "Unexpected result for DML query [" + req.sqlQuery() + "].");

                return new QuerySingleResult(cursorId, (Long)fetch.get(0).get(0));
            }
            default:
                return new QuerySingleResult(UNSUPPORTED_OPERATION,
                    "Query type [" + cur.getQueryType() + "] is not supported yet.");
        }
    }

    /**
     * Validate dml result. Check if it stores only one value of Long type.
     *
     * @param fetch Fetched data from cursor.
     * @param next HasNext flag.
     * @return Boolean value indicates if data is valid or not.
     */
    private boolean validateDmlResult(List<List<Object>> fetch, boolean next) {
        if (next)
            return false;

        if (fetch.size() != 1)
            return false;

        if (fetch.get(0).size() != 1)
            return false;

        return fetch.get(0).get(0) instanceof Long;
    }

    /**
     * Fetch next batch of data.
     *
     * @param size Batch size.
     * @param cursor Sql cursor.
     * @return Array of given size with data.
     */
    private List<List<Object>> fetchNext(int size, Cursor<List<?>> cursor) {
        List<List<Object>> fetch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (cursor.hasNext())
                fetch.add((List<Object>)cursor.next());
        }
        return fetch;
    }
}
