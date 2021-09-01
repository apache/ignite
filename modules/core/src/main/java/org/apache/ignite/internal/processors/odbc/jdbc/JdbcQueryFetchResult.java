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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query fetch result.
 */
public class JdbcQueryFetchResult extends JdbcResult {
    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcQueryFetchResult() {
        super(QRY_FETCH);
    }

    /**
     * @param items Query result rows.
     * @param last Flag indicating the query has no unfetched results.
     */
    JdbcQueryFetchResult(List<List<Object>> items, boolean last) {
        super(QRY_FETCH);

        this.items = items;
        this.last = last;
    }

    /**
     * @return Query result rows.
     */
    public List<List<Object>> items() {
        return items;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeBoolean(last);

        JdbcUtils.writeItems(writer, items, protoCtx);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        last = reader.readBoolean();

        items = JdbcUtils.readItems(reader, protoCtx);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryFetchResult.class, this);
    }
}
