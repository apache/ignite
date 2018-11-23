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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.List;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;

/**
 * We need indexes on an not affinity nodes. The index shouldn't contains any data.
 */
public class H2TreeClientIndex extends H2TreeIndexBase {
    /**
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param colsList Index columns.
     */
    public H2TreeClientIndex(
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList
    ) {

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        initDistributedJoinMessaging(tbl);
    }

    /** {@inheritDoc} */
    @Override protected int segmentsCount() {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public void refreshColumnIds() {
        super.refreshColumnIds();
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean rmvIndex) {
        super.destroy(rmvIndex);
    }

    /** {@inheritDoc} */
    @Override protected H2Tree treeForRead(int segment) {
        throw new IgniteSQLException("Shouldn't be invoked, due to it's not affinity node");
    }
}
