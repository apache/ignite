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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SystemIndexFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.table.TableBase;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * H2 Table engine.
 */
public class H2TableEngine implements TableEngine {
    /** */
    private static GridH2RowDescriptor rowDesc0;

    /** */
    private static H2RowFactory rowFactory0;

    /** */
    private static GridH2SystemIndexFactory idxFactory0;

    /** */
    private static GridH2Table resTbl0;

    /**
     * Creates table using given connection, DDL clause for given type descriptor and list of indexes.
     *
     * @param conn Connection.
     * @param sql DDL clause.
     * @param rowDesc Row descriptor.
     * @param rowFactory Row factory.
     * @param idxFactory Index factory.
     * @throws SQLException If failed.
     * @return Created table.
     */
    public static synchronized GridH2Table createTable(Connection conn, String sql,
        @Nullable GridH2RowDescriptor rowDesc, H2RowFactory rowFactory, GridH2SystemIndexFactory idxFactory)
        throws SQLException {
        rowDesc0 = rowDesc;
        rowFactory0 = rowFactory;
        idxFactory0 = idxFactory;

        try {
            try (Statement s = conn.createStatement()) {
                s.execute(sql + " engine \"" + H2TableEngine.class.getName() + "\"");
            }

            if (idxFactory instanceof H2TableDescriptor)
                ((H2TableDescriptor)idxFactory).table(resTbl0);

            return resTbl0;
        }
        finally {
            resTbl0 = null;
            idxFactory0 = null;
            rowFactory0 = null;
            rowDesc0 = null;
        }
    }

    /** {@inheritDoc} */
    @Override public TableBase createTable(CreateTableData createTblData) {
        String cacheName = null;

        if (idxFactory0 instanceof H2TableDescriptor)
            cacheName = ((H2TableDescriptor)idxFactory0).schema().cacheName();

        resTbl0 = new GridH2Table(createTblData, rowDesc0, rowFactory0, idxFactory0, cacheName);

        return resTbl0;
    }
}
