/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.table.TableBase;

/**
 * H2 Table engine.
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public class H2TableEngine implements TableEngine {
    /** */
    private static GridH2RowDescriptor rowDesc0;

    /** */
    private static H2TableDescriptor tblDesc0;

    /** */
    private static GridH2Table resTbl0;

    /**
     * Creates table using given connection, DDL clause for given type descriptor and list of indexes.
     *
     * @param conn Connection.
     * @param sql DDL clause.
     * @param rowDesc Row descriptor.
     * @param tblDesc Table descriptor.
     * @throws SQLException If failed.
     * @return Created table.
     */
    public static synchronized GridH2Table createTable(
        Connection conn,
        String sql,
        GridH2RowDescriptor rowDesc,
        H2TableDescriptor tblDesc
    )
        throws SQLException {
        rowDesc0 = rowDesc;
        tblDesc0 = tblDesc;

        try {
            try (Statement s = conn.createStatement()) {
                s.execute(sql + " engine \"" + H2TableEngine.class.getName() + "\"");
            }

            tblDesc.table(resTbl0);

            return resTbl0;
        }
        finally {
            resTbl0 = null;
            tblDesc0 = null;
            rowDesc0 = null;
        }
    }

    /** {@inheritDoc} */
    @Override public TableBase createTable(CreateTableData createTblData) {
        resTbl0 = new GridH2Table(createTblData, rowDesc0, tblDesc0, tblDesc0.cacheInfo());

        return resTbl0;
    }
}
