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

package org.apache.ignite.spi.systemview.jmx;

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
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.jmx.ReadOnlyDynamicMBean;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeVisitor;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeWithValueVisitor;

/**
 * JMX bean to expose specific {@link SystemView} data.
 *
 * @see JmxMetricExporterSpi
 * @see GridSystemViewManager
 */
public class SystemViewMBean<R> extends ReadOnlyDynamicMBean {
    /** View attribute. */
    public static final String VIEWS = "views";

    /** Row id attribute name. */
    public static final String ID = "systemViewRowId";

    /** System view to export. */
    private final SystemView<R> sysView;

    /** MBean info. */
    private final MBeanInfo info;

    /** Row type. */
    private final CompositeType rowType;

    /** System view type. */
    private final TabularType sysViewType;

    /**
     * @param sysView System view to export.
     */
    public SystemViewMBean(SystemView<R> sysView) {
        this.sysView = sysView;

        int cnt = sysView.walker().count();

        String[] fields = new String[cnt+1];
        OpenType[] types = new OpenType[cnt+1];

        sysView.walker().visitAll(new AttributeVisitor() {
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
                else if (clazz == boolean.class || clazz.isAssignableFrom(Boolean.class))
                    types[idx] = SimpleType.BOOLEAN;
                else if (clazz == byte.class || clazz.isAssignableFrom(Byte.class))
                    types[idx] = SimpleType.BYTE;
                else if (clazz == char.class || clazz.isAssignableFrom(Character.class))
                    types[idx] = SimpleType.CHARACTER;
                else if (clazz == short.class || clazz.isAssignableFrom(Short.class))
                    types[idx] = SimpleType.SHORT;
                else if (clazz == int.class || clazz.isAssignableFrom(Integer.class))
                    types[idx] = SimpleType.INTEGER;
                else if (clazz == long.class || clazz.isAssignableFrom(Long.class))
                    types[idx] = SimpleType.LONG;
                else if (clazz == float.class || clazz.isAssignableFrom(Float.class))
                    types[idx] = SimpleType.FLOAT;
                else if (clazz == double.class || clazz.isAssignableFrom(Double.class))
                    types[idx] = SimpleType.DOUBLE;
                else
                    types[idx] = SimpleType.STRING;
            }
        });

        fields[cnt] = ID;
        types[cnt] = SimpleType.INTEGER;

        try {
            rowType = new CompositeType(sysView.rowClass().getName(),
                sysView.description(),
                fields,
                fields,
                types);

            info = new OpenMBeanInfoSupport(
                sysView.rowClass().getName(),
                sysView.description(),
                new OpenMBeanAttributeInfo[] {
                    new OpenMBeanAttributeInfoSupport(VIEWS, VIEWS, rowType, true, false, false)
                },
                null,
                null,
                null
            );

            sysViewType = new TabularType(
                sysView.rowClass().getName(),
                sysView.description(),
                rowType,
                new String[] {ID}
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

        if (attribute.equals(VIEWS)) {
            TabularDataSupport rows = new TabularDataSupport(sysViewType);

            AttributeToMapVisitor visitor = new AttributeToMapVisitor();

            try {
                int idx = 0;

                for (R row : sysView) {
                    Map<String, Object> data = new HashMap<>();

                    visitor.data(data);

                    sysView.walker().visitAll(row, visitor);

                    data.put(ID, idx++);

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

    /** Fullfill {@code data} Map for specific row. */
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
                data.put(name, String.valueOf(val));
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
