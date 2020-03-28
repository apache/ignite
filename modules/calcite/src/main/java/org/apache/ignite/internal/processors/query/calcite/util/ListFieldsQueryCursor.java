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
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class ListFieldsQueryCursor<T> implements FieldsQueryCursor<List<?>>, QueryCursorEx<List<?>> {
    /** */
    private final Iterator<List<?>> it;

    /** */
    private final List<GridQueryFieldMetadata> fieldsMeta;

    /** */
    private final boolean isQuery;

    /**
     * @param plan Query plan.
     * @param it Iterator.
     * @param converter Row converter.
     */
    public ListFieldsQueryCursor(MultiStepPlan plan, Iterator<T> it, Function<T, List<?>> converter) {
        fieldsMeta = plan.fieldsMetadata();
        isQuery = plan.type() == QueryPlan.Type.QUERY;

        this.it = new ConvertingClosableIterator<>(it, converter);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<List<?>> iterator() {
        return it;
    }

    /** {@inheritDoc} */
    @Override public List<List<?>> getAll() {
        ArrayList<List<?>> res = new ArrayList<>();

        try {
            getAll(res::add);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
        return isQuery;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Commons.closeQuiet(it);
    }
}
