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
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.lang.IgniteUuid;
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
    /** System view for export. */
    private final SystemView<R> sysView;

    /**
     * @param ctx Kernal context.
     * @param sysView View to export.
     */
    public SystemViewLocal(GridKernalContext ctx, SystemView<R> sysView) {
        super(sqlName(sysView.name()), sysView.description(), ctx, columnsList(sysView));

        this.sysView = sysView;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Iterator<R> rows = sysView.iterator();

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
                        else if (clazz.isAssignableFrom(Class.class))
                            data[idx] = ValueString.get(((Class)val).getName());
                        else if (clazz.isAssignableFrom(String.class) || clazz.isEnum() ||
                            clazz.isAssignableFrom(IgniteUuid.class) ||
                            clazz.isAssignableFrom(InetSocketAddress.class))
                            data[idx] = ValueString.get(Objects.toString(val));
                        else if (clazz.isAssignableFrom(UUID.class))
                            data[idx] = ValueUuid.get((UUID)val);
                        else if (clazz.isAssignableFrom(BigDecimal.class))
                            data[idx] = ValueDecimal.get((BigDecimal)val);
                        else if (clazz.isAssignableFrom(BigInteger.class))
                            data[idx] = ValueDecimal.get(new BigDecimal((BigInteger)val));
                        else if (clazz.isAssignableFrom(Date.class))
                            data[idx] = ValueTimestamp.fromMillis(((Date)val).getTime());
                        else if (clazz.isAssignableFrom(Boolean.class))
                            data[idx] = ValueBoolean.get((Boolean)val);
                        else if (clazz.isAssignableFrom(Byte.class))
                            data[idx] = ValueByte.get((Byte)val);
                        else if (clazz.isAssignableFrom(Character.class))
                            data[idx] = ValueString.get(Objects.toString(val));
                        else if (clazz.isAssignableFrom(Short.class))
                            data[idx] = ValueShort.get((Short)val);
                        else if (clazz.isAssignableFrom(Integer.class))
                            data[idx] = ValueInt.get((Integer)val);
                        else if (clazz.isAssignableFrom(Long.class))
                            data[idx] = ValueLong.get((Long)val);
                        else if (clazz.isAssignableFrom(Float.class))
                            data[idx] = ValueFloat.get((Float)val);
                        else if (clazz.isAssignableFrom(Double.class))
                            data[idx] = ValueDouble.get((Double)val);
                        else
                            data[idx] = ValueString.get(val.toString());
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
     * @return SQL column array for {@code rowClass}.
     * @see SystemView#rowClass()
     */
    private static <R> Column[] columnsList(SystemView<R> sysView) {
        Column[] cols = new Column[sysView.walker().count()];

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                int type;

                if (clazz.isAssignableFrom(String.class) || clazz.isEnum() ||
                    clazz.isAssignableFrom(IgniteUuid.class) ||
                    clazz.isAssignableFrom(Class.class) || clazz.isAssignableFrom(InetSocketAddress.class))
                    type = Value.STRING;
                else if (clazz.isAssignableFrom(UUID.class))
                    type = Value.UUID;
                else if (clazz.isAssignableFrom(BigDecimal.class))
                    type = Value.DECIMAL;
                else if (clazz.isAssignableFrom(BigInteger.class))
                    type = Value.DECIMAL;
                else if (clazz.isAssignableFrom(Date.class))
                    type = Value.TIMESTAMP;
                else if (clazz == boolean.class || clazz.isAssignableFrom(Boolean.class))
                    type = Value.BOOLEAN;
                else if (clazz == byte.class || clazz.isAssignableFrom(Byte.class))
                    type = Value.BYTE;
                else if (clazz == char.class || clazz.isAssignableFrom(Character.class))
                    type = Value.STRING;
                else if (clazz == short.class || clazz.isAssignableFrom(Short.class))
                    type = Value.SHORT;
                else if (clazz == int.class || clazz.isAssignableFrom(Integer.class))
                    type = Value.INT;
                else if (clazz == long.class || clazz.isAssignableFrom(Long.class))
                    type = Value.LONG;
                else if (clazz == float.class || clazz.isAssignableFrom(Float.class))
                    type = Value.FLOAT;
                else if (clazz == double.class || clazz.isAssignableFrom(Double.class))
                    type = Value.DOUBLE;
                else
                    type = Value.STRING;

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
    public static String sqlName(String name) {
        return name
            .replaceAll("([A-Z])", "_$1")
            .replaceAll('\\' + MetricUtils.SEPARATOR, "_").toUpperCase();
    }
}
