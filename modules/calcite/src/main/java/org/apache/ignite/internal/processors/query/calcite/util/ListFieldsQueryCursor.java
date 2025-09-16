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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.ExecutionNodeMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.MemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.RowTracker;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class ListFieldsQueryCursor<Row> implements FieldsQueryCursor<List<?>>, QueryCursorEx<List<?>> {
    /** */
    private final Iterator<List<?>> it;

    /** */
    private final List<GridQueryFieldMetadata> fieldsMeta;

    /** */
    private final MemoryTracker qryMemoryTracker;

    /** */
    private final boolean isQry;

    /**
     * @param plan Query plan.
     * @param it Iterator.
     * @param ectx Row converter.
     * @param qryMemoryTracker Query memory tracker.
     */
    public ListFieldsQueryCursor(
        MultiStepPlan plan,
        Iterator<List<?>> it,
        ExecutionContext<Row> ectx,
        MemoryTracker qryMemoryTracker
    ) {
        FieldsMetadata metadata0 = plan.fieldsMetadata();
        assert metadata0 != null;
        fieldsMeta = metadata0.queryFieldsMetadata(ectx.getTypeFactory());
        isQry = plan.type() == QueryPlan.Type.QUERY;
        this.qryMemoryTracker = qryMemoryTracker;
        this.it = it;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<List<?>> iterator() {
        return it;
    }

    /** {@inheritDoc} */
    @Override public List<List<?>> getAll() {
        ArrayList<List<?>> res = new ArrayList<>();

        RowTracker<List<?>> rowTracker = ExecutionNodeMemoryTracker.create(qryMemoryTracker,
            GridUnsafe.OBJ_REF_SIZE);

        try {
            getAll(row -> {
                rowTracker.onRowAdded(row);
                res.add(row);
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            qryMemoryTracker.reset();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void getAll(Consumer<List<?>> c) throws IgniteCheckedException {
        try {
            while (it.hasNext())
                c.consume(it.next());
        }
        finally {
            close();
        }
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> fieldsMeta() {
        return fieldsMeta;
    }

    /** {@inheritDoc} */
    @Override public String getFieldName(int idx) {
        return fieldsMeta.get(idx).fieldName();
    }

    /** {@inheritDoc} */
    @Override public int getColumnsCount() {
        return fieldsMeta.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuery() {
        return isQry;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Commons.closeQuiet(it);
    }
}
