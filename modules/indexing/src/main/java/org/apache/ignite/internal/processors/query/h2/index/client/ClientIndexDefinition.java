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

package org.apache.ignite.internal.processors.query.h2.index.client;

import java.util.List;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/**
 * Define index for filtered or client node.
 */
public class ClientIndexDefinition implements IndexDefinition {
    /** */
    private final int cfgInlineSize;

    /** */
    private final int maxInlineSize;

    /** */
    private final IndexName idxName;

    /** */
    private final List<IndexColumn> cols;

    /** */
    private final GridH2Table table;

    /** */
    public ClientIndexDefinition(GridH2Table table, IndexName idxName, List<IndexColumn> unwrappedCols,
        int cfgInlineSize, int maxInlineSize) {
        this.table = table;
        this.idxName = idxName;
        this.cfgInlineSize = cfgInlineSize;
        this.maxInlineSize = maxInlineSize;
        cols = unwrappedCols;
    }

    /** */
    public GridH2Table getTable() {
        return table;
    }

    /** */
    public List<IndexColumn> getColumns() {
        return cols;
    }

    /** */
    public int getCfgInlineSize() {
        return cfgInlineSize;
    }

    /** */
    public int getMaxInlineSize() {
        return maxInlineSize;
    }

    /** {@inheritDoc} */
    @Override public IndexName idxName() {
        return idxName;
    }
}
