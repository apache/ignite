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

package org.apache.ignite.internal.jdbc2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.IgniteEx;

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
     * @param sql  SQL query.
     * @param streamer Data streamer to use with this statement. Will be closed on statement close.
     */
    JdbcStreamedPreparedStatement(JdbcConnection conn, String sql, IgniteDataStreamer<?, ?> streamer,
        PreparedStatement nativeStmt) {
        super(conn, sql);

        this.streamer = streamer;

        nativeStatement = nativeStmt;
    }

    /** {@inheritDoc} */
    @Override void closeInternal() throws SQLException {
        streamer.close(false);

        super.closeInternal();
    }

    /** {@inheritDoc} */
    @Override long doUpdate(String sql, Object[] args) throws SQLException {
        return conn.ignite().context().query().streamUpdateQuery(conn.cacheName(), conn.schemaName(),
            streamer, sql, args);
    }
}
