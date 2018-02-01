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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.bulkload.BulkLoadParameters;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * A request from server (in form of reply) to send files from client to server,
 * which is sent as a response to SQL COPY command (see IGNITE-6917 for details).
 *
 * @see SqlBulkLoadCommand
 */
public class JdbcBulkLoadBatchRequestResult extends JdbcResult {
    /** Query ID for matching this command on server in further {@link JdbcBulkLoadBatchRequest} commands. */
    private long queryId;

    /**
     * Bulk load parameters, which are parsed on the server side and sent to client to specify
     * what files to send, batch size, etc. */
    private BulkLoadParameters params;

    /**Creates uninitialized bulk load batch request result. */
    public JdbcBulkLoadBatchRequestResult() {
        super(BULK_LOAD_BATCH_REQUEST);

        queryId = 0;
        params = null;
    }

    /**
     * Constructs a request from server (in form of reply) to send files from client to server.
     *
     * @param queryId Query ID to send in further {@link JdbcBulkLoadBatchRequest}-s.
     * @param params Various parameters for sending batches from client side.
     */
    public JdbcBulkLoadBatchRequestResult(long queryId, BulkLoadParameters params) {
        super(BULK_LOAD_BATCH_REQUEST);

        this.queryId = queryId;
        this.params = params;
    }

    /**
     * Returns the query ID.
     *
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * Returns the parameters for the client.
     *
     * @return The parameters for the client.
     */
    public BulkLoadParameters params() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeLong(queryId);
        writer.writeString(params.localFileName());
        writer.writeInt(params.batchSize());
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        queryId = reader.readLong();

        String locFileName = reader.readString();
        int batchSize = reader.readInt();

        if (!BulkLoadParameters.isValidBatchSize(batchSize))
            throw new BinaryObjectException(BulkLoadParameters.batchSizeErrorMsg(batchSize));

        params = new BulkLoadParameters(locFileName, batchSize);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBulkLoadBatchRequestResult.class, this);
    }
}
