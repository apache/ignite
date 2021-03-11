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

package org.apache.ignite.internal.processors.query.h2.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.cache.query.index.NullsOrder;
import org.apache.ignite.internal.cache.query.index.Order;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/** Maps H2 columns to IndexKeyDefinition and InlineIndexKeyType. */
public class QueryIndexKeyDefinitionProvider {
    /** Table. */
    private final GridH2Table table;

    /** H2 index columns. */
    private final List<IndexColumn> h2IdxColumns;

    /** Unmodified list of index key definitions. */
    private List<IndexKeyDefinition> keyDefs;

    /** */
    public QueryIndexKeyDefinitionProvider(GridH2Table table, List<IndexColumn> h2IdxColumns) {
        this.table = table;
        this.h2IdxColumns = h2IdxColumns;
    }

    /**
     * @return List of index key definitions.
     */
    public List<IndexKeyDefinition> keyDefinitions() {
        if (keyDefs != null)
            return keyDefs;

        List<IndexKeyDefinition> idxKeyDefinitions = new ArrayList<>();

        for (IndexColumn c: h2IdxColumns)
            idxKeyDefinitions.add(keyDefinition(c));

        IndexColumn.mapColumns(h2IdxColumns.toArray(new IndexColumn[0]), table);

        keyDefs = Collections.unmodifiableList(idxKeyDefinitions);

        return keyDefs;
    }

    /** */
    private IndexKeyDefinition keyDefinition(IndexColumn c) {
        return new IndexKeyDefinition(
            c.columnName, c.column.getType(), sortOrder(c.sortType));
    }

    /** Maps H2 column order to Ignite index order. */
    private Order sortOrder(int sortType) {
        SortOrder sortOrder = (sortType & 1) != 0 ? SortOrder.DESC : SortOrder.ASC;

        NullsOrder nullsOrder = (sortType & 2) != 0 ? NullsOrder.NULLS_FIRST : NullsOrder.NULLS_LAST;

        return new Order(sortOrder, nullsOrder);
    }
}
