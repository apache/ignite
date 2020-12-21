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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Runtime sorted index based on on-heap tree.
 */
public class RuntimeTreeIndex<Row> extends AbstractRuntimeSortedIndex<Row> {
    /** Collation. */
    private final RelCollation collation;

    /** Rows. */
    private TreeMap<Row, List<Row>> rows;

    /**
     *
     */
    public RuntimeTreeIndex(
        ExecutionContext<Row> ectx,
        RelCollation collation,
        Comparator<Row> comp
    ) {
        super(ectx, comp);

        assert Objects.nonNull(collation);

        this.collation = collation;
        rows = new TreeMap<>(comp);
    }

    /** */
    public void push(Row r) {
        List<Row> eqRows = rows.putIfAbsent(r, new ArrayList<>(Collections.singletonList(r)));

        if (eqRows != null)
            eqRows.add(r);
    }

    /** */
    @Override public void close() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCursor<Row> find(Row lower, Row upper, BPlusTree.TreeRowClosure<Row, Row> filterC) {
        return find(lower, upper);
    }

    /** */
    private GridCursor<Row> find(Row lower, Row upper) {
        int firstCol = F.first(collation.getKeys());

        if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) != null)
            return new Cursor(rows.subMap(lower, true, upper, true));
        else if (ectx.rowHandler().get(firstCol, lower) == null && ectx.rowHandler().get(firstCol, upper) != null)
            return new Cursor(rows.headMap(upper, true));
        else if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) == null)
            return new Cursor(rows.tailMap(lower, true));
        else
            return new Cursor(rows);
    }

    /**
     *
     */
    private class Cursor implements GridCursor<Row> {
        /** Sub map. */
        private final Iterator<Map.Entry<Row, List<Row>>> mapIt;

        /** Sub map. */
        private Iterator<Row> listIt;

        /** */
        private Row row;

        /** */
        public Cursor(SortedMap<Row, List<Row>> subMap) {
            mapIt = subMap.entrySet().iterator();
            listIt = null;
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (!hasNext())
                return false;

            next0();

            return true;
        }

        /** */
        private boolean hasNext() {
            return listIt != null && listIt.hasNext() || mapIt.hasNext();
        }

        /** */
        private void next0() {
            if (listIt != null && listIt.hasNext())
                row = listIt.next();
            else {
                listIt = mapIt.next().getValue().iterator();

                row = listIt.next();
            }
        }

        /** {@inheritDoc} */
        @Override public Row get() throws IgniteCheckedException {
            return row;
        }
    }
}
