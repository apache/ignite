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

package org.apache.ignite.internal.processors.query.h2.sys;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.table.Table;

/**
 * H2 table engine for system views.
 */
public class SqlSystemTableEngine implements TableEngine {
    /** View being created. */
    private static volatile SqlSystemView curView;

    /**
     * @param conn Connection.
     * @param view View.
     */
    public static synchronized void registerView(Connection conn, SqlSystemView view)
        throws SQLException {
        curView = view;

        String sql = view.getCreateSQL() + " ENGINE \"" + SqlSystemTableEngine.class.getName() + "\"";

        try {
            try (Statement s = conn.createStatement()) {
                s.execute(sql);
            }
        }
        finally {
            curView = null;
        }
    }

    /** {@inheritDoc} */
    @Override public Table createTable(CreateTableData data) {
        return new SystemViewH2Adapter(data, curView);
    }
}
