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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewSchemas extends SqlAbstractLocalSystemView {
    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewSchemas(GridKernalContext ctx, SchemaManager schemaMgr) {
        super("SCHEMAS", "Ignite SQL schemas", ctx,
            newColumn("SCHEMA_NAME")
        );

        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Set<String> schemaNames = schemaMgr.schemaNames();

        List<Row> rows = new ArrayList<>(schemaNames.size());

        for (String schemaName : schemaNames)
            rows.add(createRow(ses, schemaName));

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return schemaMgr.schemaNames().size();
    }
}
