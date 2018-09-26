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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query execute result for query with multiple SQL statements.
 */
public class JdbcQueryExecuteMultipleStatementsResult extends JdbcResult {
    /** Statements results. */
    private List<JdbcResultInfo> results;

    /** Query result rows for the first query. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results for the first query. */
    private boolean last;

    /**
     * Default constructor.
     */
    JdbcQueryExecuteMultipleStatementsResult() {
        super(QRY_EXEC_MULT);
    }

    /**
     * @param results Statements results.
     * @param items Query result rows for the first query.
     * @param last Flag indicating the query has no unfetched results for the first query.
     */
    public JdbcQueryExecuteMultipleStatementsResult(List<JdbcResultInfo> results,
        List<List<Object>> items, boolean last) {
        super(QRY_EXEC_MULT);
        this.results = results;
        this.items = items;
        this.last = last;
    }

    /**
     * @return Update counts of query IDs.
     */
    public List<JdbcResultInfo> results() {
        return results;
    }

    /**
     * @return Query result rows for the first query.
     */
    public List<List<Object>> items() {
        return items;
    }

    /**
     * @return Flag indicating the query has no unfetched results for the first query.
     */
    public boolean isLast() {
        return last;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        if (results != null && !results.isEmpty()) {
            writer.writeInt(results.size());

            for (JdbcResultInfo r : results)
                r.writeBinary(writer, ver);

            if (results.get(0).isQuery()) {
                writer.writeBoolean(last);

                JdbcUtils.writeItems(writer, items);
            }
        }
        else
            writer.writeInt(0);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        int cnt = reader.readInt();

        if (cnt == 0)
            results = Collections.emptyList();
        else {
            results = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; ++i) {
                JdbcResultInfo r = new JdbcResultInfo();

                r.readBinary(reader, ver);

                results.add(r);
            }

            if (results.get(0).isQuery()) {
                last = reader.readBoolean();

                items = JdbcUtils.readItems(reader);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteMultipleStatementsResult.class, this);
    }
}
