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

package org.apache.ignite.internal.jdbc2;

import java.sql.SQLException;
import java.util.Collections;
import org.apache.ignite.IgniteDataStreamer;

/**
 * Prepared statement associated with a data streamer.
 */
class JdbcStreamedPreparedStatement extends JdbcPreparedStatement {
    /** */
    private final IgniteDataStreamer<?, ?> streamer;

    /**
     * Creates new prepared statement.
     *
     * @param conn Connection.
     * @param sql SQL query.
     * @param streamer Data streamer to use with this statement. Will be closed on statement close.
     */
    JdbcStreamedPreparedStatement(JdbcConnection conn, String sql, IgniteDataStreamer<?, ?> streamer) {
        super(conn, sql);

        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override void closeInternal() throws SQLException {
        streamer.close(false);

        super.closeInternal();
    }

    /** {@inheritDoc} */
    @Override protected void execute0(String sql, Boolean isQuery) throws SQLException {
        assert isQuery == null || !isQuery;

        long updCnt = conn.ignite().context().query().streamUpdateQuery(conn.cacheName(), conn.schemaName(),
            streamer, sql, getArgs());

        JdbcResultSet rs = new JdbcResultSet(this, updCnt);

        results = Collections.singletonList(rs);

        curRes = 0;
    }
}
