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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOffheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SimpleRow;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.DEFAULT_COLUMNS_COUNT;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.VAL_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.VER_COL;

/**
 * Row descriptor.
 */
public class H2RowDescriptor implements GridH2RowDescriptor {
    /** Indexing SPI. */
    private final IgniteH2Indexing idx;

    /** */
    private final GridQueryTypeDescriptor type;

    /** */
    private final String[] fields;

    /** */
    private final int[] fieldTypes;

    /** */
    private final int keyType;

    /** */
    private final int valType;

    /** */
    private final H2Schema schema;

    /** */
    private final GridUnsafeGuard guard;

    /** */
    private final boolean snapshotableIdx;

    /** */
    private final GridQueryProperty[] props;

    /** Id of user-defined key column */
    private final int keyAliasColumnId;

    /** Id of user-defined value column */
    private final int valueAliasColumnId;

    /**
     * @param type Type descriptor.
     * @param schema Schema.
     */
    H2RowDescriptor(IgniteH2Indexing idx, GridQueryTypeDescriptor type, H2Schema schema) {
        assert type != null;
        assert schema != null;

        this.idx = idx;
        this.type = type;
        this.schema = schema;

        guard = schema.offheap() == null ? null : new GridUnsafeGuard();

        Map<String, Class<?>> allFields = new LinkedHashMap<>();

        allFields.putAll(type.fields());

        fields = allFields.keySet().toArray(new String[allFields.size()]);

        fieldTypes = new int[fields.length];

        Class[] classes = allFields.values().toArray(new Class[fields.length]);

        for (int i = 0; i < fieldTypes.length; i++)
            fieldTypes[i] = DataType.getTypeFromClass(classes[i]);

        keyType = DataType.getTypeFromClass(type.keyClass());
        valType = DataType.getTypeFromClass(type.valueClass());

        props = new GridQueryProperty[fields.length];

        for (int i = 0; i < fields.length; i++) {
            GridQueryProperty p = type.property(fields[i]);

            assert p != null : fields[i];

            props[i] = p;
        }

        final List<String> fieldsList = Arrays.asList(fields);

        keyAliasColumnId =
            (type.keyFieldName() != null) ? DEFAULT_COLUMNS_COUNT + fieldsList.indexOf(type.keyFieldAlias()) : -1;

        valueAliasColumnId =
            (type.valueFieldName() != null) ? DEFAULT_COLUMNS_COUNT + fieldsList.indexOf(type.valueFieldAlias()) : -1;

        // Index is not snapshotable in db-x.
        snapshotableIdx = false;
    }

    /** {@inheritDoc} */
    @Override public IgniteH2Indexing indexing() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<?, ?> context() {
        return schema.cacheContext();
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration configuration() {
        return schema.cacheContext().config();
    }

    /** {@inheritDoc} */
    @Override public GridUnsafeGuard guard() {
        return guard;
    }

    /** {@inheritDoc} */
    @Override public void cache(GridH2Row row) {
        long ptr = row.pointer();

        assert ptr > 0 : ptr;

        schema.rowCache().put(ptr, row);
    }

    /** {@inheritDoc} */
    @Override public void uncache(long ptr) {
        schema.rowCache().remove(ptr);
    }

    /** {@inheritDoc} */
    @Override public GridUnsafeMemory memory() {
        return schema.offheap();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public Value wrap(Object obj, int type) throws IgniteCheckedException {
        assert obj != null;

        if (obj instanceof CacheObject) { // Handle cache object.
            CacheObject co = (CacheObject)obj;

            if (type == Value.JAVA_OBJECT)
                return new GridH2ValueCacheObject(co, idx.objectContext());

            obj = co.value(idx.objectContext(), false);
        }

        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get((Boolean)obj);
            case Value.BYTE:
                return ValueByte.get((Byte)obj);
            case Value.SHORT:
                return ValueShort.get((Short)obj);
            case Value.INT:
                return ValueInt.get((Integer)obj);
            case Value.FLOAT:
                return ValueFloat.get((Float)obj);
            case Value.LONG:
                return ValueLong.get((Long)obj);
            case Value.DOUBLE:
                return ValueDouble.get((Double)obj);
            case Value.UUID:
                UUID uuid = (UUID)obj;
                return ValueUuid.get(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            case Value.DATE:
                return ValueDate.get((Date)obj);
            case Value.TIME:
                return ValueTime.get((Time)obj);
            case Value.TIMESTAMP:
                if (obj instanceof java.util.Date && !(obj instanceof Timestamp))
                    obj = new Timestamp(((java.util.Date)obj).getTime());

                return ValueTimestamp.get((Timestamp)obj);
            case Value.DECIMAL:
                return ValueDecimal.get((BigDecimal)obj);
            case Value.STRING:
                return ValueString.get(obj.toString());
            case Value.BYTES:
                return ValueBytes.get((byte[])obj);
            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(obj, null, null);
            case Value.ARRAY:
                Object[] arr = (Object[])obj;

                Value[] valArr = new Value[arr.length];

                for (int i = 0; i < arr.length; i++) {
                    Object o = arr[i];

                    valArr[i] = o == null ? ValueNull.INSTANCE : wrap(o, DataType.getTypeFromClass(o.getClass()));
                }

                return ValueArray.get(valArr);

            case Value.GEOMETRY:
                return ValueGeometry.getFromGeometry(obj);
        }

        throw new IgniteCheckedException("Failed to wrap value[type=" + type + ", value=" + obj + "]");
    }

    /** {@inheritDoc} */
    @Override public GridH2Row createRow(KeyCacheObject key, int partId, @Nullable CacheObject val,
        GridCacheVersion ver, long expirationTime) throws IgniteCheckedException {
        GridH2Row row;

        try {
            if (val == null) // Only can happen for remove operation, can create simple search row.
                row = GridH2RowFactory.create(wrap(key, keyType));
            else
                row = schema.offheap() == null ?
                    new GridH2KeyValueRowOnheap(this, key, keyType, val, valType, ver, expirationTime) :
                    new GridH2KeyValueRowOffheap(this, key, keyType, val, valType, ver, expirationTime);
        }
        catch (ClassCastException e) {
            throw new IgniteCheckedException("Failed to convert key to SQL type. " +
                "Please make sure that you always store each value type with the same key type " +
                "or configure key type as common super class for all actual keys for this value type.", e);
        }

        row.ver = ver;

        row.key = key;
        row.val = val;
        row.partId = partId;

        return row;
    }

    /** {@inheritDoc} */
    @Override public int valueType() {
        return valType;
    }

    /** {@inheritDoc} */
    @Override public int fieldsCount() {
        return fields.length;
    }

    /** {@inheritDoc} */
    @Override public int fieldType(int col) {
        return fieldTypes[col];
    }

    /** {@inheritDoc} */
    @Override public Object columnValue(Object key, Object val, int col) {
        try {
            return props[col].value(key, val);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setColumnValue(Object key, Object val, Object colVal, int col) {
        try {
            props[col].setValue(key, val, colVal);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isColumnKeyProperty(int col) {
        return props[col].key();
    }

    /** {@inheritDoc} */
    @Override public GridH2KeyValueRowOffheap createPointer(long ptr) {
        GridH2KeyValueRowOffheap row = (GridH2KeyValueRowOffheap)schema.rowCache().get(ptr);

        if (row != null) {
            assert row.pointer() == ptr : ptr + " " + row.pointer();

            return row;
        }

        return new GridH2KeyValueRowOffheap(this, ptr);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row cachedRow(long link) {
        return schema.rowCache().get(link);
    }

    /** {@inheritDoc} */
    @Override public boolean snapshotableIndex() {
        return snapshotableIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean isKeyColumn(int columnId) {
        assert columnId >= 0;
        return columnId == KEY_COL || columnId == keyAliasColumnId;
    }

    /** {@inheritDoc} */
    @Override public boolean isValueColumn(int columnId) {
        assert columnId >= 0;
        return columnId == VAL_COL || columnId == valueAliasColumnId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("RedundantIfStatement")
    @Override public boolean isKeyValueOrVersionColumn(int columnId) {
        assert columnId >= 0;

        if (columnId < DEFAULT_COLUMNS_COUNT)
            return true;

        if (columnId == keyAliasColumnId)
            return true;

        if (columnId == valueAliasColumnId)
            return true;

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean checkKeyIndexCondition(int masks[], int mask) {
        assert masks != null;
        assert masks.length > 0;

        if (keyAliasColumnId < 0)
            return (masks[KEY_COL] & mask) != 0;
        else
            return (masks[KEY_COL] & mask) != 0 || (masks[keyAliasColumnId] & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public void initValueCache(Value valCache[], Value key, Value value, Value version) {
        assert valCache != null;
        assert valCache.length > 0;

        valCache[KEY_COL] = key;
        valCache[VAL_COL] = value;
        valCache[VER_COL] = version;

        if (keyAliasColumnId > 0)
            valCache[keyAliasColumnId] = key;

        if (valueAliasColumnId > 0)
            valCache[valueAliasColumnId] = value;
    }

    /** {@inheritDoc} */
    @Override public SearchRow prepareProxyIndexRow(SearchRow row) {
        if (row == null)
            return null;

        Value[] data = new Value[row.getColumnCount()];
        for (int idx = 0; idx < data.length; idx++)
            data[idx] = row.getValue(idx);

        copyAliasColumnData(data, KEY_COL, keyAliasColumnId);
        copyAliasColumnData(data, VAL_COL, valueAliasColumnId);

        return new SimpleRow(data);
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

    /** {@inheritDoc} */
    @Override public int getAlternativeColumnId(int colId) {
        if (keyAliasColumnId > 0) {
            if (colId == KEY_COL)
                return keyAliasColumnId;
            else if (colId == keyAliasColumnId)
                return KEY_COL;
        }
        if (valueAliasColumnId > 0) {
            if (colId == VAL_COL)
                return valueAliasColumnId;
            else if (colId == valueAliasColumnId)
                return VAL_COL;
        }

        return colId;
    }
}
