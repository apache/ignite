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

package org.apache.ignite.spi.systemview;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeVisitor;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeWithValueVisitor;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * SQL system view to export {@link SystemView} data.
 */
public class SystemViewLocal<R> extends SqlAbstractLocalSystemView {

    private static final Map<Class<?>, Function<Object, ? extends Value>> CLASS_TO_VALUE_MAP = new HashMap<>();

    private static final Map<Class<?>, Integer> CLASS_TO_VALUE_TYPE_RECORDS = new HashMap<>();

    static {
        registerClassToValueRecords();
        registerClassToValueTypeRecords();
    }

    /** Maps classes to Value representation. */
    private static void registerClassToValueTypeRecords() {
        CLASS_TO_VALUE_TYPE_RECORDS.put(String.class, Value.STRING);
        CLASS_TO_VALUE_TYPE_RECORDS.put(IgniteUuid.class, Value.STRING);
        CLASS_TO_VALUE_TYPE_RECORDS.put(UUID.class, Value.UUID);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Class.class, Value.STRING);
        CLASS_TO_VALUE_TYPE_RECORDS.put(InetSocketAddress.class, Value.STRING);
        CLASS_TO_VALUE_TYPE_RECORDS.put(BigDecimal.class, Value.DECIMAL);
        CLASS_TO_VALUE_TYPE_RECORDS.put(BigInteger.class, Value.DECIMAL);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Date.class, Value.DATE);
        CLASS_TO_VALUE_TYPE_RECORDS.put(boolean.class, Value.BOOLEAN);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Boolean.class, Value.BOOLEAN);
        CLASS_TO_VALUE_TYPE_RECORDS.put(byte.class, Value.BYTE);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Byte.class, Value.BYTE);
        CLASS_TO_VALUE_TYPE_RECORDS.put(short.class, Value.SHORT);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Short.class, Value.SHORT);
        CLASS_TO_VALUE_TYPE_RECORDS.put(int.class, Value.INT);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Integer.class, Value.INT);
        CLASS_TO_VALUE_TYPE_RECORDS.put(long.class, Value.LONG);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Long.class, Value.LONG);
        CLASS_TO_VALUE_TYPE_RECORDS.put(char.class, Value.STRING);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Character.class, Value.STRING);
        CLASS_TO_VALUE_TYPE_RECORDS.put(float.class, Value.FLOAT);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Float.class, Value.FLOAT);
        CLASS_TO_VALUE_TYPE_RECORDS.put(double.class, Value.DOUBLE);
        CLASS_TO_VALUE_TYPE_RECORDS.put(Double.class, Value.DOUBLE);
    }

    /** Maps values by their classes according to their (classes) Value representation. */
    private static void registerClassToValueRecords() {
        CLASS_TO_VALUE_MAP.put(String.class, val -> ValueString.get(Objects.toString(val)));
        CLASS_TO_VALUE_MAP.put(IgniteUuid.class, val -> ValueString.get(Objects.toString(val)));
        CLASS_TO_VALUE_MAP.put(UUID.class, val -> ValueUuid.get((UUID) val));
        CLASS_TO_VALUE_MAP.put(Class.class, val -> ValueString.get(((Class<?>) val).getName()));
        CLASS_TO_VALUE_MAP.put(InetSocketAddress.class, val -> ValueString.get(Objects.toString(val)));
        CLASS_TO_VALUE_MAP.put(BigDecimal.class, val -> ValueDecimal.get((BigDecimal) val));
        CLASS_TO_VALUE_MAP.put(BigInteger.class, val -> ValueDecimal.get(new BigDecimal((BigInteger) val)));
        CLASS_TO_VALUE_MAP.put(Date.class, val -> ValueTimestamp.fromMillis(((Date) val).getTime()));
        CLASS_TO_VALUE_MAP.put(Boolean.class, val -> ValueBoolean.get((Boolean) val));
        CLASS_TO_VALUE_MAP.put(Byte.class, val -> ValueByte.get((Byte) val));
        CLASS_TO_VALUE_MAP.put(Short.class, val -> ValueShort.get((Short) val));
        CLASS_TO_VALUE_MAP.put(Integer.class, val -> ValueInt.get((Integer) val));
        CLASS_TO_VALUE_MAP.put(Long.class, val -> ValueLong.get((Long) val));
        CLASS_TO_VALUE_MAP.put(Character.class, val -> ValueString.get(Objects.toString(val)));
        CLASS_TO_VALUE_MAP.put(Float.class, val -> ValueFloat.get((Float) val));
        CLASS_TO_VALUE_MAP.put(Double.class, val -> ValueDouble.get((Double) val));
        CLASS_TO_VALUE_MAP.put(null, val -> ValueNull.INSTANCE);
    }

    /** System view for export. */
    protected final SystemView<R> sysView;

    /**
     * @param ctx Kernal context.
     * @param sysView View to export.
     * @param indexes Indexed fields.
     */
    protected SystemViewLocal(GridKernalContext ctx, SystemView<R> sysView, String[] indexes) {
        super(sqlName(sysView.name()), sysView.description(), ctx, indexes, columnsList(sysView));

        this.sysView = sysView;
    }

    /**
     * @param ctx Kernal context.
     * @param sysView View to export.
     */
    public SystemViewLocal(GridKernalContext ctx, SystemView<R> sysView) {
        this(ctx, sysView, null);
    }

    /**
     * System view iterator.
     */
    protected Iterator<R> viewIterator(SearchRow first, SearchRow last) {
        return sysView.iterator();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Iterator<R> rows = viewIterator(first, last);

        return new Iterator<Row>() {
            @Override public boolean hasNext() {
                return rows.hasNext();
            }

            @Override public Row next() {
                R row = rows.next();

                Value[] data = new Value[sysView.walker().count()];

                sysView.walker().visitAll(row, new AttributeWithValueVisitor() {
                    @Override public <T> void accept(int idx, String name, Class<T> clazz, T val) {
                        data[idx] = CLASS_TO_VALUE_MAP
                                .getOrDefault(clazz, value -> ValueString.get(value.toString())).apply(val);
                    }

                    @Override public void acceptBoolean(int idx, String name, boolean val) {
                        data[idx] = ValueBoolean.get(val);
                    }

                    @Override public void acceptChar(int idx, String name, char val) {
                        data[idx] = ValueString.get(Character.toString(val));
                    }

                    @Override public void acceptByte(int idx, String name, byte val) {
                        data[idx] = ValueByte.get(val);
                    }

                    @Override public void acceptShort(int idx, String name, short val) {
                        data[idx] = ValueShort.get(val);
                    }

                    @Override public void acceptInt(int idx, String name, int val) {
                        data[idx] = ValueInt.get(val);
                    }

                    @Override public void acceptLong(int idx, String name, long val) {
                        data[idx] = ValueLong.get(val);
                    }

                    @Override public void acceptFloat(int idx, String name, float val) {
                        data[idx] = ValueFloat.get(val);
                    }

                    @Override public void acceptDouble(int idx, String name, double val) {
                        data[idx] = ValueDouble.get(val);
                    }
                });

                return createRow(ses, data);
            }
        };
    }

    /**
     * Extract column array for specific {@link SystemView}.
     *
     * @param sysView System view.
     * @param <R> Row type.
     * @return SQL column array for {@code sysView}.
     */
    private static <R> Column[] columnsList(SystemView<R> sysView) {
        Column[] cols = new Column[sysView.walker().count()];

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                int type = CLASS_TO_VALUE_TYPE_RECORDS.getOrDefault(clazz, Value.STRING);
                cols[idx] = newColumn(sqlName(name), type);
            }
        });

        return cols;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return sysView.size();
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        // getRowCount() method is not really fast, for some system views it's required to iterate over elements to
        // calculate size, so it's more safe to use constant here.
        return DEFAULT_ROW_COUNT_APPROXIMATION;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /**
     * Build SQL-like name from Java code style name.
     * Some examples:
     *
     * cacheName -> CACHE_NAME.
     * affinitiKeyName -> AFFINITY_KEY_NAME.
     *
     * @param name Name to convert.
     * @return SQL compatible name.
     */
    protected static String sqlName(String name) {
        return name
            .replaceAll("([A-Z])", "_$1")
            .replaceAll('\\' + MetricUtils.SEPARATOR, "_").toUpperCase();
    }
}
