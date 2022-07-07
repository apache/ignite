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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Row descriptor.
 */
public class GridQueryRowDescriptorImpl implements GridQueryRowDescriptor {
    /** Non existent column. */
    public static final int COL_NOT_EXISTS = -1;

    /**  */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** */
    private final GridQueryTypeDescriptor type;

    /** */
    private volatile String[] fields;

    /** */
    private volatile GridQueryProperty[] props;

    /** */
    private volatile Set<String> rowKeyColumnNames;

    /** Id of user-defined key column */
    private volatile int keyAliasColId;

    /** Id of user-defined value column */
    private volatile int valAliasColId;

    /**
     * Constructor.
     *
     * @param cacheInfo Cache context.
     * @param type Type descriptor.
     */
    public GridQueryRowDescriptorImpl(GridCacheContextInfo<?, ?> cacheInfo, GridQueryTypeDescriptor type) {
        assert type != null;

        this.cacheInfo = cacheInfo;
        this.type = type;

        onMetadataUpdated();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"WeakerAccess", "ToArrayCallWithZeroLengthArrayArgument"})
    @Override public final void onMetadataUpdated() {
        fields = type.fields().keySet().toArray(new String[type.fields().size()]);

        props = new GridQueryProperty[fields.length];

        for (int i = 0; i < fields.length; i++) {
            GridQueryProperty p = type.property(fields[i]);

            assert p != null : fields[i];

            props[i] = p;
        }

        List<String> fieldsList = Arrays.asList(fields);

        keyAliasColId = (type.keyFieldName() != null) ?
            QueryUtils.DEFAULT_COLUMNS_COUNT + fieldsList.indexOf(type.keyFieldAlias()) : COL_NOT_EXISTS;

        valAliasColId = (type.valueFieldName() != null) ?
            QueryUtils.DEFAULT_COLUMNS_COUNT + fieldsList.indexOf(type.valueFieldAlias()) : COL_NOT_EXISTS;
    
        rowKeyColumnNames = Arrays.stream(props).filter(GridQueryProperty::key)
                .map(GridQueryProperty::name)
                .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheContext<?, ?> context() {
        return cacheInfo.cacheContext();
    }

    /** {@inheritDoc} */
    @Override public int fieldsCount() {
        return fields.length;
    }

    /** {@inheritDoc} */
    @Override public Object getFieldValue(Object key, Object val, int fieldIdx) {
        try {
            return props[fieldIdx].value(key, val);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setFieldValue(Object key, Object val, Object fieldVal, int fieldIdx) {
        try {
            props[fieldIdx].setValue(key, val, fieldVal);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isFieldKeyProperty(int fieldIdx) {
        return props[fieldIdx].key();
    }

    /** {@inheritDoc} */
    @Override public boolean isKeyColumn(int colId) {
        assert colId >= 0;
        return colId == QueryUtils.KEY_COL || colId == keyAliasColId;
    }

    /** {@inheritDoc} */
    @Override public boolean isValueColumn(int colId) {
        assert colId >= 0;
        return colId == QueryUtils.VAL_COL || colId == valAliasColId;
    }

    /** {@inheritDoc} */
    @Override public int getAlternativeColumnId(int colId) {
        if (keyAliasColId > 0) {
            if (colId == QueryUtils.KEY_COL)
                return keyAliasColId;
            else if (colId == keyAliasColId)
                return QueryUtils.KEY_COL;
        }
        if (valAliasColId > 0) {
            if (colId == QueryUtils.VAL_COL)
                return valAliasColId;
            else if (colId == valAliasColId)
                return QueryUtils.VAL_COL;
        }

        return colId;
    }
    
    /** {@inheritDoc} */
    @Override public Set<String> getRowKeyColumnNames() {
        return new HashSet<>(rowKeyColumnNames);
    }
}
