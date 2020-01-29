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

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class ListFieldsQueryCursor<T> implements FieldsQueryCursor<List<?>> {
    /** */
    private final RelDataType rowType;

    /** */
    private final Iterator<T> it;

    /** */
    private final Function<T, List<?>> converter;

    /**
     * @param rowType Row data type description.
     * @param it Iterator.
     * @param converter Row converter.
     */
    public ListFieldsQueryCursor(RelDataType rowType, Iterator<T> it, Function<T, List<?>> converter) {
        this.rowType = rowType;
        this.it = it;
        this.converter = converter;
    }

    /** {@inheritDoc} */
    @Override public String getFieldName(int idx) {
        return rowType.getFieldList().get(idx).getName();
    }

    /** {@inheritDoc} */
    @Override public int getColumnsCount() {
        return rowType.getFieldCount();
    }

    /** {@inheritDoc} */
    @Override public List<List<?>> getAll() {
        try {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false)
                .map(converter)
                .collect(Collectors.toList());
        }
        finally {
            close();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Commons.close(it);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<List<?>> iterator() {
        return F.iterator(it, converter::apply, true);
    }
}
