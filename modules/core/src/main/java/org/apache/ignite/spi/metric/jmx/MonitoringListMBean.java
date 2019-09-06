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

package org.apache.ignite.spi.metric.jmx;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfo;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker.AttributeVisitor;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker.AttributeWithValueVisitor;

/**
 * JMX bean to expose specific {@link MonitoringList} data.
 *
 * @see JmxMetricExporterSpi
 * @see GridMetricManager
 */
public class MonitoringListMBean<Id, R extends MonitoringRow<Id>> extends ReadOnlyDynamicMBean {
    /** List attribute.  */
    public static final String LIST = "list";

    /** Monitoring list to export. */
    private final MonitoringList<Id, R> mlist;

    /** MBean info. */
    private MBeanInfo info;

    /** Row type */
    private CompositeType rowType;

    /** List type. */
    private TabularType listType;

    /**
     * @param mlist Monitoring list to export.
     */
    public MonitoringListMBean(MonitoringList<Id, R> mlist) {
        this.mlist = mlist;

        int cnt = mlist.walker().count();

        String[] fields = new String[cnt+1];
        OpenType[] types = new OpenType[cnt+1];

        mlist.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                fields[idx] = name;

                if (clazz.isAssignableFrom(String.class) || clazz.isEnum() ||
                    clazz.isAssignableFrom(IgniteUuid.class) || clazz.isAssignableFrom(UUID.class) ||
                    clazz.isAssignableFrom(Class.class) || clazz.isAssignableFrom(InetSocketAddress.class))
                    types[idx] = SimpleType.STRING;
                else if (clazz.isAssignableFrom(BigDecimal.class))
                    types[idx] = SimpleType.BIGDECIMAL;
                else if (clazz.isAssignableFrom(BigInteger.class))
                    types[idx] = SimpleType.BIGINTEGER;
                else if (clazz.isAssignableFrom(Date.class))
                    types[idx] = SimpleType.DATE;
                else if (clazz.isAssignableFrom(ObjectName.class))
                    types[idx] = SimpleType.OBJECTNAME;
                else if (clazz.isAssignableFrom(Boolean.class))
                    types[idx] = SimpleType.BOOLEAN;
                else if (clazz.isAssignableFrom(Byte.class))
                    types[idx] = SimpleType.BYTE;
                else if (clazz.isAssignableFrom(Character.class))
                    types[idx] = SimpleType.CHARACTER;
                else if (clazz.isAssignableFrom(Short.class))
                    types[idx] = SimpleType.SHORT;
                else if (clazz.isAssignableFrom(Integer.class))
                    types[idx] = SimpleType.INTEGER;
                else if (clazz.isAssignableFrom(Long.class))
                    types[idx] = SimpleType.LONG;
                else if (clazz.isAssignableFrom(Float.class))
                    types[idx] = SimpleType.FLOAT;
                else if (clazz.isAssignableFrom(Double.class))
                    types[idx] = SimpleType.DOUBLE;
                else {
                    throw new IllegalStateException(
                        "Unsupported type [rowClass=" + mlist.rowClass().getName() + ",col=" + name);
                }
            }

            @Override public void acceptBoolean(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.BOOLEAN;
            }

            @Override public void acceptChar(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.CHARACTER;
            }

            @Override public void acceptByte(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.BYTE;
            }

            @Override public void acceptShort(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.SHORT;
            }

            @Override public void acceptInt(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.INTEGER;
            }

            @Override public void acceptLong(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.LONG;
            }

            @Override public void acceptFloat(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.FLOAT;
            }

            @Override public void acceptDouble(int idx, String name) {
                fields[idx] = name;
                types[idx] = SimpleType.DOUBLE;
            }
        });

        fields[cnt] = "monitoringRowId";
        types[cnt] = SimpleType.INTEGER;

        try {
            rowType = new CompositeType(mlist.rowClass().getName(),
                mlist.description(),
                fields,
                fields,
                types);

            info = new OpenMBeanInfoSupport(
                mlist.rowClass().getName(),
                mlist.description(),
                new OpenMBeanAttributeInfo[] {
                    new OpenMBeanAttributeInfoSupport(LIST, LIST, rowType, true, false, false)
                },
                null,
                null,
                null
            );

            listType = new TabularType(
                mlist.rowClass().getName(),
                mlist.description(),
                rowType,
                new String[] {"monitoringRowId"}
            );
        }
        catch (OpenDataException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(String attribute) {
        if ("MBeanInfo".equals(attribute))
            return getMBeanInfo();

        if (attribute.equals(LIST)) {
            TabularDataSupport rows = new TabularDataSupport(listType);

            AttributeToMapVisitor visitor = new AttributeToMapVisitor();

            try {
                int idx = 0;

                for (R row : mlist) {
                    Map<String, Object> data = new HashMap<>();

                    visitor.data(data);

                    mlist.walker().visitAllWithValues(row, visitor);

                    data.put("monitoringRowId", idx++);

                    rows.put(new CompositeDataSupport(rowType, data));
                }
            }
            catch (OpenDataException e) {
                throw new IgniteException(e);
            }

            return rows;
        }

        throw new IllegalArgumentException("Unknown attribute " + attribute);
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        return info;
    }

    /**
     * Fullfill {@code data} Map for specific {@link MonitoringRow}.
     */
    private static class AttributeToMapVisitor implements AttributeWithValueVisitor {
        /** Map to store data. */
        private Map<String, Object> data;

        /**
         * Sets map.
         *
         * @param data Map to fill.
         */
        public void data(Map<String, Object> data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz, T val) {
            if (clazz.isEnum())
                data.put(name, ((Enum)val).name());
            else if (clazz.isAssignableFrom(Class.class))
                data.put(name, ((Class<?>)val).getName());
            else if (clazz.isAssignableFrom(IgniteUuid.class) || clazz.isAssignableFrom(UUID.class) ||
                clazz.isAssignableFrom(InetSocketAddress.class))
                data.put(name, val == null ? "null" : val.toString());
            else
                data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptBoolean(int idx, String name, boolean val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptChar(int idx, String name, char val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptByte(int idx, String name, byte val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptShort(int idx, String name, short val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptInt(int idx, String name, int val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptLong(int idx, String name, long val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptFloat(int idx, String name, float val) {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override public void acceptDouble(int idx, String name, double val) {
            data.put(name, val);
        }
    }
}
