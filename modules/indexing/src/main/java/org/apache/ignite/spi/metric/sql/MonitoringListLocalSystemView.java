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

package org.apache.ignite.spi.metric.sql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker.AttributeVisitor;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker.AttributeWithValueVisitor;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * System view to export monitoring list data.
 */
public class MonitoringListLocalSystemView<Id, R extends MonitoringRow<Id>> extends SqlAbstractLocalSystemView {
    private final MonitoringList<Id, R> mlist;

    /**
     * @param ctx Kernal context.
     * @param mlist List to export.
     */
    public MonitoringListLocalSystemView(GridKernalContext ctx, MonitoringList<Id, R> mlist) {
        super(sqlName(mlist.name()), mlist.description(), ctx, columnsList(mlist));

        this.mlist = mlist;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Iterator<R> rows = mlist.iterator();

        return new Iterator<Row>() {
            @Override public boolean hasNext() {
                return rows.hasNext();
            }

            @Override public Row next() {
                R row = rows.next();

                Object[] data = new Object[mlist.walker().count()];

                mlist.walker().visitAllWithValues(row, new AttributeWithValueVisitor() {
                    @Override public <T> void accept(int idx, String name, Class<T> clazz, T val) {
                        if (clazz.isAssignableFrom(String.class) || clazz.isEnum() ||
                            clazz.isAssignableFrom(IgniteUuid.class) || clazz.isAssignableFrom(UUID.class) ||
                            clazz.isAssignableFrom(Class.class) || clazz.isAssignableFrom(InetSocketAddress.class))
                            data[idx] = Objects.toString(val);
                        else
                            data[idx] = val;
                    }

                    @Override public void acceptBoolean(int idx, String name, boolean val) {
                        data[idx] = val;
                    }

                    @Override public void acceptChar(int idx, String name, char val) {
                        data[idx] = val;
                    }

                    @Override public void acceptByte(int idx, String name, byte val) {
                        data[idx] = val;
                    }

                    @Override public void acceptShort(int idx, String name, short val) {
                        data[idx] = val;
                    }

                    @Override public void acceptInt(int idx, String name, int val) {
                        data[idx] = val;
                    }

                    @Override public void acceptLong(int idx, String name, long val) {
                        data[idx] = val;
                    }

                    @Override public void acceptFloat(int idx, String name, float val) {
                        data[idx] = val;
                    }

                    @Override public void acceptDouble(int idx, String name, double val) {
                        data[idx] = val;
                    }
                });

                return createRow(ses, data);
            }
        };
    }

    /**
     *
     * @param mlist Monitoring list.
     * @param <R> Row type.
     * @return SQL column list for {@code rowClass}.
     */
    private static <Id, R extends MonitoringRow<Id>> Column[] columnsList(MonitoringList<Id, R> mlist) {
        Column[] cols = new Column[mlist.walker().count()];

        mlist.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                int type;

                if (clazz.isAssignableFrom(String.class) || clazz.isEnum() ||
                    clazz.isAssignableFrom(IgniteUuid.class) || clazz.isAssignableFrom(UUID.class) ||
                    clazz.isAssignableFrom(Class.class) || clazz.isAssignableFrom(InetSocketAddress.class))
                    type = Value.STRING;
                else if (clazz.isAssignableFrom(BigDecimal.class))
                    type = Value.DECIMAL;
                else if (clazz.isAssignableFrom(BigInteger.class))
                    type = Value.DECIMAL;
                else if (clazz.isAssignableFrom(Date.class))
                    type = Value.DATE;
                else if (clazz.isAssignableFrom(Boolean.class))
                    type = Value.BOOLEAN;
                else if (clazz.isAssignableFrom(Byte.class))
                    type = Value.BYTE;
                else if (clazz.isAssignableFrom(Character.class))
                    type = Value.STRING;
                else if (clazz.isAssignableFrom(Short.class))
                    type = Value.SHORT;
                else if (clazz.isAssignableFrom(Integer.class))
                    type = Value.INT;
                else if (clazz.isAssignableFrom(Long.class))
                    type = Value.LONG;
                else if (clazz.isAssignableFrom(Float.class))
                    type = Value.FLOAT;
                else if (clazz.isAssignableFrom(Double.class))
                    type = Value.DOUBLE;
                else {
                    throw new IllegalStateException
                        ("Unsupported type [rowClass=" + mlist.rowClass().getName() + ",col=" + name);
                }

                cols[idx] = newColumn(sqlName(name), type);
            }

            @Override public void acceptBoolean(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.BOOLEAN);
            }

            @Override public void acceptChar(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.STRING);
            }

            @Override public void acceptByte(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.BYTE);
            }

            @Override public void acceptShort(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.SHORT);
            }

            @Override public void acceptInt(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.INT);
            }

            @Override public void acceptLong(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.LONG);
            }

            @Override public void acceptFloat(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.FLOAT);
            }

            @Override public void acceptDouble(int idx, String name) {
                cols[idx] = newColumn(sqlName(name), Value.DOUBLE);
            }
        });

        return cols;
    }

    /**
     * @param name Name to convert
     * @return SQL compatible name.
     */
    private static String sqlName(String name) {
        return name.replaceAll('\\' + MetricUtils.SEPARATOR, "_").toUpperCase();
    }
}
