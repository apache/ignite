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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Row descriptor.
 */
public class GridH2RowDescriptor {
    /** Non existent column. */
    public static final int COL_NOT_EXISTS = -1;

    /** Table descriptor. */
    private final H2TableDescriptor tbl;

    /** */
    private final GridQueryTypeDescriptor type;

    /** */
    private volatile String[] fields;

    /** */
    private volatile int[] fieldTypes;

    /** */
    private final int keyType;

    /** */
    private final int valType;

    /** */
    private volatile GridQueryProperty[] props;

    /** Id of user-defined key column */
    private volatile int keyAliasColId;

    /** Id of user-defined value column */
    private volatile int valAliasColId;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param type Type descriptor.
     */
    public GridH2RowDescriptor(H2TableDescriptor tbl, GridQueryTypeDescriptor type) {
        assert type != null;

        this.tbl = tbl;
        this.type = type;

        keyType = DataType.getTypeFromClass(type.keyClass());
        valType = DataType.getTypeFromClass(type.valueClass());

        refreshMetadataFromTypeDescriptor();
    }

    /**
     * Gets table descriptor.
     *
     * @return Table descriptor.
     */
    public H2TableDescriptor tableDescriptor() {
        return tbl;
    }

    /**
     * Update metadata of this row descriptor according to current state of type descriptor.
     */
    @SuppressWarnings({"WeakerAccess", "ToArrayCallWithZeroLengthArrayArgument"})
    public final void refreshMetadataFromTypeDescriptor() {

        Map<String, Class<?>> allFields = new LinkedHashMap<>(type.fields());

        fields = allFields.keySet().toArray(new String[allFields.size()]);

        fieldTypes = new int[fields.length];

        Class[] classes = allFields.values().toArray(new Class[fields.length]);

        for (int i = 0; i < fieldTypes.length; i++)
            fieldTypes[i] = DataType.getTypeFromClass(classes[i]);

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
    }

    /**
     * Gets indexing.
     *
     * @return indexing.
     */
    public IgniteH2Indexing indexing() {
        return tbl.indexing();
    }

    /**
     * Gets type descriptor.
     *
     * @return Type descriptor.
     */
    public GridQueryTypeDescriptor type() {
        return type;
    }

    /**
     * Gets cache context info for this row descriptor.
     *
     * @return Cache context info.
     */
    public GridCacheContextInfo<?, ?> cacheInfo() {
        return tbl.cacheInfo();
    }

    /**
     * Gets cache context for this row descriptor.
     *
     * @return Cache context.
     */
    @Nullable public GridCacheContext<?, ?> context() {
        return tbl.cache();
    }

    /**
     * Create new row for update operation.
     *
     * @param dataRow Data row.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public H2CacheRow createRow(CacheDataRow dataRow) throws IgniteCheckedException {
        H2CacheRow row;

        try {
            row = new H2CacheRow(this, dataRow);
        }
        catch (ClassCastException e) {
            throw new IgniteCheckedException("Failed to convert key to SQL type. " +
                "Please make sure that you always store each value type with the same key type " +
                "or configure key type as common super class for all actual keys for this value type.", e);
        }

        return row;
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
     * @return Total fields count.
     */
    public int fieldsCount() {
        return fields.length;
    }

    /**
     * Gets value type for column index.
     *
     * @param col Column index.
     * @return Value type.
     */
    public int fieldType(int col) {
        return fieldTypes[col];
    }

    /**
     * Gets column value by column index.
     *
     * @param key Key.
     * @param val Value.
     * @param col Column index.
     * @return  Column value.
     */
    public Object columnValue(Object key, Object val, int col) {
        try {
            return props[col].value(key, val);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Gets column value by column index.
     *
     * @param key Key.
     * @param val Value.
     * @param colVal Value to set to column.
     * @param col Column index.
     */
    public void setColumnValue(Object key, Object val, Object colVal, int col) {
        try {
            props[col].setValue(key, val, colVal);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Determine whether a column corresponds to a property of key or to one of value.
     *
     * @param col Column index.
     * @return {@code true} if given column corresponds to a key property, {@code false} otherwise
     */
    public boolean isColumnKeyProperty(int col) {
        return props[col].key();
    }

    /**
     * Checks if provided column id matches key column or key alias.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isKeyColumn(int colId) {
        assert colId >= 0;
        return colId == QueryUtils.KEY_COL || colId == keyAliasColId;
    }

    /**
     * Checks if provided column id matches key alias column.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isKeyAliasColumn(int colId) {
        assert colId >= 0;
        return colId == keyAliasColId;
    }

    /**
     * Checks if provided column id matches value column or alias.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isValueColumn(int colId) {
        assert colId >= 0;
        return colId == QueryUtils.VAL_COL || colId == valAliasColId;
    }

    /**
     * Checks if provided column id matches value alias column.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isValueAliasColumn(int colId) {
        assert colId >= 0;
        return colId == valAliasColId;
    }

    /**
     * Checks if provided column id matches key, key alias,
     * value, value alias or version column.
     *
     * @param colId Column id.
     * @return Result.
     */
    @SuppressWarnings("RedundantIfStatement")
    public boolean isKeyValueOrVersionColumn(int colId) {
        assert colId >= 0;

        if (colId < QueryUtils.DEFAULT_COLUMNS_COUNT)
            return true;

        if (colId == keyAliasColId)
            return true;

        if (colId == valAliasColId)
            return true;

        return false;
    }

    /**
     * Checks if provided index condition is allowed for key column or key alias column.
     *
     * @param masks Array containing Index Condition masks for each column.
     * @param mask Index Condition to check.
     * @return Result.
     */
    @SuppressWarnings("IfMayBeConditional")
    public boolean checkKeyIndexCondition(int masks[], int mask) {
        assert masks != null;
        assert masks.length > 0;

        if (keyAliasColId < 0)
            return (masks[QueryUtils.KEY_COL] & mask) != 0;
        else
            return (masks[QueryUtils.KEY_COL] & mask) != 0 || (masks[keyAliasColId] & mask) != 0;
    }

    /**
     * Clones provided row and copies values of alias key and val columns
     * into respective key and val positions.
     *
     * @param row Source row.
     * @return Result.
     */
    public SearchRow prepareProxyIndexRow(SearchRow row) {
        if (row == null)
            return null;

        Value[] data = new Value[row.getColumnCount()];

        for (int idx = 0; idx < data.length; idx++)
            data[idx] = row.getValue(idx);

        copyAliasColumnData(data, QueryUtils.KEY_COL, keyAliasColId);
        copyAliasColumnData(data, QueryUtils.VAL_COL, valAliasColId);

        return H2PlainRowFactory.create(data);
    }

    /**
     * Copies data between original and alias columns
     *
     * @param data Array of values.
     * @param colId Original column id.
     * @param aliasColId Alias column id.
     */
    private void copyAliasColumnData(Value[] data, int colId, int aliasColId) {
        if (aliasColId <= 0)
            return;

        if (data[aliasColId] == null && data[colId] != null)
            data[aliasColId] = data[colId];

        if (data[colId] == null && data[aliasColId] != null)
            data[colId] = data[aliasColId];
    }

    /**
     * Gets alternative column id that may substitute the given column id.
     *
     * For alias column returns original one.
     * For original column returns its alias.
     *
     * Otherwise, returns the given column id.
     *
     * @param colId Column id.
     * @return Result.
     */
    public int getAlternativeColumnId(int colId) {
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
}
