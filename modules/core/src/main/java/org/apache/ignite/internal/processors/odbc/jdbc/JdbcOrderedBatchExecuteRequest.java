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

import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * JDBC batch execute ordered request.
 */
public class JdbcOrderedBatchExecuteRequest extends JdbcBatchExecuteRequest
    implements Comparable<JdbcOrderedBatchExecuteRequest> {
    /** Order. */
    private long order;

    /**
     * Default constructor.
     */
    public JdbcOrderedBatchExecuteRequest() {
        super(BATCH_EXEC_ORDERED);
    }

    /**
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param autoCommit Client auto commit flag state.
     * @param lastStreamBatch {@code true} in case the request is the last batch at the stream.
     * @param order Request order.
     */
    public JdbcOrderedBatchExecuteRequest(String schemaName, List<JdbcQuery> queries,
        boolean autoCommit, boolean lastStreamBatch, long order) {
        super(BATCH_EXEC_ORDERED, schemaName, queries, autoCommit, lastStreamBatch);

        this.order = order;
    }

    /**
     * @return Request order.
     */
    public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeLong(order);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        order = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcOrderedBatchExecuteRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull JdbcOrderedBatchExecuteRequest o) {
        return Long.compare(order, o.order);
    }
}
