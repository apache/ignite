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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryRowDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.h2.value.DataType;
import org.jetbrains.annotations.Nullable;

/**
 * Row descriptor.
 */
public class GridH2RowDescriptor implements GridQueryRowDescriptor {
    /** */
    private volatile int[] fieldTypes;

    /** */
    private final int keyType;

    /** */
    private final int valType;

    /** */
    private final GridQueryRowDescriptor delegate;

    /**
     * Ctor.
     *
     * @param delegate Delegate.
     */
    public GridH2RowDescriptor(GridQueryRowDescriptor delegate) {
        this.delegate = delegate;

        keyType = DataType.getTypeFromClass(delegate.type().keyClass());
        valType = DataType.getTypeFromClass(delegate.type().valueClass());

        updateFieldTypes();
    }

    /** {@inheritDoc} */
    @Override public void onMetadataUpdated() {
        delegate.onMetadataUpdated();

        updateFieldTypes();
    }

    /** */
    private void updateFieldTypes() {
        Collection<Class<?>> classes = delegate.type().fields().values();

        fieldTypes = new int[classes.size()];

        int fieldIdx = 0;

        for (Class<?> cls : classes)
            fieldTypes[fieldIdx++] = DataType.getTypeFromClass(cls);
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor type() {
        return delegate.type();
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheContext<?, ?> context() {
        return delegate.context();
    }

    /**
     * @return Key type.
     */
    public int keyType() {
        return keyType;
    }

    /**
     * @return Value type.
     */
    public int valueType() {
        return valType;
    }

    /**
     * Gets value type for field index.
     *
     * @param fieldIdx Field index.
     * @return Value type.
     */
    public int fieldType(int fieldIdx) {
        return fieldTypes[fieldIdx];
    }

    /** {@inheritDoc} */
    @Override public int fieldsCount() {
        return delegate.fieldsCount();
    }

    /** {@inheritDoc} */
    @Override public Object getFieldValue(Object key, Object val, int fieldIdx) {
        return delegate.getFieldValue(key, val, fieldIdx);
    }

    /** {@inheritDoc} */
    @Override public void setFieldValue(Object key, Object val, Object fieldVal, int fieldIdx) {
        delegate.setFieldValue(key, val, fieldVal, fieldIdx);
    }

    /** {@inheritDoc} */
    @Override public boolean isFieldKeyProperty(int fieldIdx) {
        return delegate.isFieldKeyProperty(fieldIdx);
    }

    /** {@inheritDoc} */
    @Override public boolean isKeyColumn(int colId) {
        return delegate.isKeyColumn(colId);
    }

    /** {@inheritDoc} */
    @Override public boolean isValueColumn(int colId) {
        return delegate.isValueColumn(colId);
    }

    /** {@inheritDoc} */
    @Override public int getAlternativeColumnId(int colId) {
        return delegate.getAlternativeColumnId(colId);
    }

    /** {@inheritDoc} */
    @Override public Set<String> getRowKeyColumnNames() {
        return delegate.getRowKeyColumnNames();
    }
}
