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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query execute result for query with multiple SQL statements.
 */
public class JdbcQueryExecuteMultipleStatementsResult extends JdbcResult {
    /** Statements results. */
    private List<JdbcStatementResults> results;

    /**
     * Default constructor.
     */
    JdbcQueryExecuteMultipleStatementsResult() {
        super(QRY_EXEC_MULT);
    }

    /**
     * @param results Statements results.
     */
    public JdbcQueryExecuteMultipleStatementsResult(List<JdbcStatementResults> results) {
        super(QRY_EXEC_MULT);
        this.results = results;
    }

    /**
     * @return Update counts of query IDs.
     */
    public List<JdbcStatementResults> results() {
        return results;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        if (results != null && results.size() > 0) {
            writer.writeInt(results.size());

            for (JdbcStatementResults r : results)
                r.writeBinary(writer);

        } else
            writer.writeInt(0);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        int cnt = reader.readInt();
        if (cnt == 0)
            results = Collections.emptyList();
        else
        {
            results = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; ++i) {
                JdbcStatementResults r = new JdbcStatementResults();

                r.readBinary(reader);

                results.add(r);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteMultipleStatementsResult.class, this);
    }
}
