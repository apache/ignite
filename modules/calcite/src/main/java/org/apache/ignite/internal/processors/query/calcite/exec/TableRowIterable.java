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
package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;

/**
 * Interface to iterate over raw table data and form relational node rows from table row columns.
 *
 * @param <TableRow> Raw table row type.
 * @param <Row> Relational node row type.
 */
public interface TableRowIterable<TableRow, Row> extends Iterable<Row> {
    /**
     * @return Table row iterator.
     */
    public Iterator<TableRow> tableRowIterator();

    /**
     * Enriches {@code nodeRow} with columns from {@code tableRow}.
     *
     * @param tableRow Table row.
     * @param nodeRow Relational node row.
     * @param fieldColMapping Mapping from node row fields to table row columns. If column is not requried
     *        corresponding field has -1 mapped value.
     * @return Enriched relational node row.
     */
    public Row enrichRow(TableRow tableRow, Row nodeRow, int[] fieldColMapping);
}
