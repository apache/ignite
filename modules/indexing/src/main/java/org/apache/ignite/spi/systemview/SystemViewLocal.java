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
    /** */
    private static final Map<Class<?>, Function<Object, ? extends Value>> CLS_TO_VAL = new HashMap<>();

    /** */
    private static final Function<Object, ? extends Value> DFLT_FUNC = val -> ValueString.get(Objects.toString(val));

    /** */
    private static final Map<Class<?>, Integer> CLS_TO_VAL_TYPE = new HashMap<>();

    static {
        CLS_TO_VAL_TYPE.put(String.class, Value.STRING);
        CLS_TO_VAL_TYPE.put(IgniteUuid.class, Value.STRING);
        CLS_TO_VAL_TYPE.put(UUID.class, Value.UUID);
        CLS_TO_VAL_TYPE.put(Class.class, Value.STRING);
        CLS_TO_VAL_TYPE.put(InetSocketAddress.class, Value.STRING);
        CLS_TO_VAL_TYPE.put(BigDecimal.class, Value.DECIMAL);
        CLS_TO_VAL_TYPE.put(BigInteger.class, Value.DECIMAL);
        CLS_TO_VAL_TYPE.put(Date.class, Value.TIMESTAMP);
        CLS_TO_VAL_TYPE.put(boolean.class, Value.BOOLEAN);
        CLS_TO_VAL_TYPE.put(Boolean.class, Value.BOOLEAN);
        CLS_TO_VAL_TYPE.put(byte.class, Value.BYTE);
        CLS_TO_VAL_TYPE.put(Byte.class, Value.BYTE);
        CLS_TO_VAL_TYPE.put(short.class, Value.SHORT);
        CLS_TO_VAL_TYPE.put(Short.class, Value.SHORT);
        CLS_TO_VAL_TYPE.put(int.class, Value.INT);
        CLS_TO_VAL_TYPE.put(Integer.class, Value.INT);
        CLS_TO_VAL_TYPE.put(long.class, Value.LONG);
        CLS_TO_VAL_TYPE.put(Long.class, Value.LONG);
        CLS_TO_VAL_TYPE.put(char.class, Value.STRING);
        CLS_TO_VAL_TYPE.put(Character.class, Value.STRING);
        CLS_TO_VAL_TYPE.put(float.class, Value.FLOAT);
        CLS_TO_VAL_TYPE.put(Float.class, Value.FLOAT);
        CLS_TO_VAL_TYPE.put(double.class, Value.DOUBLE);
        CLS_TO_VAL_TYPE.put(Double.class, Value.DOUBLE);

        CLS_TO_VAL.put(String.class, val -> ValueString.get(Objects.toString(val)));
        CLS_TO_VAL.put(IgniteUuid.class, val -> ValueString.get(Objects.toString(val)));
        CLS_TO_VAL.put(UUID.class, val -> ValueUuid.get((UUID) val));
        CLS_TO_VAL.put(Class.class, val -> ValueString.get(((Class<?>) val).getName()));
        CLS_TO_VAL.put(InetSocketAddress.class, val -> ValueString.get(Objects.toString(val)));
        CLS_TO_VAL.put(BigDecimal.class, val -> ValueDecimal.get((BigDecimal) val));
        CLS_TO_VAL.put(BigInteger.class, val -> ValueDecimal.get(new BigDecimal((BigInteger) val)));
        CLS_TO_VAL.put(Date.class, val -> ValueTimestamp.fromMillis(((Date) val).getTime()));
        CLS_TO_VAL.put(Boolean.class, val -> ValueBoolean.get((Boolean) val));
        CLS_TO_VAL.put(Byte.class, val -> ValueByte.get((Byte) val));
        CLS_TO_VAL.put(Short.class, val -> ValueShort.get((Short) val));
        CLS_TO_VAL.put(Integer.class, val -> ValueInt.get((Integer) val));
        CLS_TO_VAL.put(Long.class, val -> ValueLong.get((Long) val));
        CLS_TO_VAL.put(Character.class, val -> ValueString.get(Objects.toString(val)));
        CLS_TO_VAL.put(Float.class, val -> ValueFloat.get((Float) val));
        CLS_TO_VAL.put(Double.class, val -> ValueDouble.get((Double) val));
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
                        if (val == null)
                            data[idx] = ValueNull.INSTANCE;
                        else
                            data[idx] = CLS_TO_VAL.getOrDefault(clazz, DFLT_FUNC).apply(val);
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
                int type = CLS_TO_VAL_TYPE.getOrDefault(clazz, Value.STRING);

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
