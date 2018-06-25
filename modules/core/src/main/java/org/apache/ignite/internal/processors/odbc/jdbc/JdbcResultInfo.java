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

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC statement result information. Keeps statement type (SELECT or UPDATE) and
 * cursorId or update count (depends on statement type).
 */
public class JdbcResultInfo implements JdbcRawBinarylizable {
    /** Query flag. */
    private boolean isQuery;

    /** Update count. */
    private long updCnt;

    /** Cursor ID. */
    private long curId;

    /**
     * Default constructor is used for serialization.
     */
    JdbcResultInfo() {
        // No-op.
    }

    /**
     * @param isQuery Query flag.
     * @param updCnt Update count.
     * @param curId  Cursor ID.
     */
    public JdbcResultInfo(boolean isQuery, long updCnt, long curId) {
        this.isQuery = isQuery;
        this.updCnt = updCnt;
        this.curId = curId;
    }

    /**
     * @return Query flag.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * @return Query ID.
     */
    public long cursorId() {
        return curId;
    }

    /**
     * @return Update count.
     */
    public long updateCount() {
        return updCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) {
        writer.writeBoolean(isQuery);
        writer.writeLong(updCnt);
        writer.writeLong(curId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) {
        isQuery = reader.readBoolean();
        updCnt = reader.readLong();
        curId = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcResultInfo.class, this);
    }
}
