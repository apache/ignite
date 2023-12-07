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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeCondition;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.schema.SystemViewColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.SystemViewTableDescriptorImpl;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.Nullable;

/** */
public class SystemViewScan<Row, ViewRow> implements Iterable<Row> {
    /** */
    private final ExecutionContext<Row> ectx;

    /** */
    private final SystemViewTableDescriptorImpl<ViewRow> desc;

    /** */
    private final RowFactory<Row> factory;

    /** */
    private final RangeIterable<Row> ranges;

    /** Participating colunms. */
    private final ImmutableBitSet requiredColumns;

    /** System view field names (for filtering). */
    private final String[] filterableFieldNames;

    /** System view field types (for filtering). */
    private final Class<?>[] filterableFieldTypes;

    /** */
    public SystemViewScan(
        ExecutionContext<Row> ectx,
        SystemViewTableDescriptorImpl<ViewRow> desc,
        @Nullable RangeIterable<Row> ranges,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        this.ectx = ectx;
        this.desc = desc;
        this.ranges = ranges;
        this.requiredColumns = requiredColumns;

        RelDataType rowType = desc.rowType(ectx.getTypeFactory(), requiredColumns);

        factory = ectx.rowHandler().factory(ectx.getTypeFactory(), rowType);

        filterableFieldNames = new String[desc.columnDescriptors().size()];
        filterableFieldTypes = new Class<?>[desc.columnDescriptors().size()];

        if (desc.isFiltrable()) {
            for (SystemViewColumnDescriptor col : desc.columnDescriptors()) {
                if (col.isFiltrable()) {
                    filterableFieldNames[col.fieldIndex()] = col.originalName();
                    filterableFieldTypes[col.fieldIndex()] = col.storageType();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> iterator() {
        SystemView<ViewRow> view = desc.systemView();

        Iterator<ViewRow> viewIter;

        if (ranges != null) {
            assert view instanceof FiltrableSystemView : view;

            Iterator<RangeCondition<Row>> rangesIter = ranges.iterator();
            RangeCondition<Row> range = rangesIter.next();

            assert !rangesIter.hasNext();

            Row searchValues = range.lower(); // Lower bound for the hash index should be the same as upper bound.

            RowHandler<Row> rowHnd = ectx.rowHandler();
            Map<String, Object> filterMap = null;

            for (int i = 0; i < filterableFieldNames.length; i++) {
                if (filterableFieldNames[i] == null)
                    continue;

                Object val = rowHnd.get(i, searchValues);

                if (val != ectx.unspecifiedValue()) {
                    if (filterMap == null)
                        filterMap = new HashMap<>();

                    filterMap.put(filterableFieldNames[i], TypeUtils.fromInternal(ectx, val, filterableFieldTypes[i]));
                }
            }

            viewIter = F.isEmpty(filterMap) ? view.iterator() : ((FiltrableSystemView<ViewRow>)view).iterator(filterMap);
        }
        else
            viewIter = view.iterator();

        return F.iterator(viewIter, row -> desc.toRow(ectx, row, factory, requiredColumns), true);
    }
}
