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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query ID intermediate result. Used for send query Id on the client before the results to
 * allow cancel heavy and long running queries.
 */
public class JdbcQueryIdResult extends JdbcResult {
    /** Query ID. */
    private long queryId;

    /**
     * Condtructor.
     */
    JdbcQueryIdResult() {
        super(QRY_ID);
    }

    /**
     * @param queryId Query ID.
     */
    JdbcQueryIdResult(long queryId) {
        super(QRY_ID);

        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeLong(queryId);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        queryId = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryIdResult.class, this);
    }
}
