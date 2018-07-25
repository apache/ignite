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

package org.apache.ignite.internal.processors.query.h2.views;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.table.Table;

/**
 * Meta view H2 table engine.
 */
public class SqlMetaTableEngine implements TableEngine {
    /** */
    private static volatile SqlMetaView view;

    /**
     * @param conn Connection.
     * @param view View.
     */
    public static synchronized void registerView(Connection conn, SqlMetaView view)
        throws SQLException {
        SqlMetaTableEngine.view = view;

        String sql = view.getCreateSQL() + " ENGINE \"" + SqlMetaTableEngine.class.getName() + "\"";

        try {
            try (Statement s = conn.createStatement()) {
                s.execute(sql);
            }
        }
        finally {
            SqlMetaTableEngine.view = null;
        }
    }

    /** {@inheritDoc} */
    @Override public Table createTable(CreateTableData data) {
        return new SqlMetaTable(data, view);
    }
}
