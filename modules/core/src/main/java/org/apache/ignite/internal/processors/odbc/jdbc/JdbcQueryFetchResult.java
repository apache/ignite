/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    JdbcQueryFetchResult(List<List<Object>> items, boolean last){
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
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeBoolean(last);

        JdbcUtils.writeItems(writer, items);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        last = reader.readBoolean();

        items = JdbcUtils.readItems(reader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryFetchResult.class, this);
    }
}
