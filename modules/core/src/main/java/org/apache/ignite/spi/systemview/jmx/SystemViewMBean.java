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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfo;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.OpenMBeanOperationInfo;
import javax.management.openmbean.OpenMBeanOperationInfoSupport;
import javax.management.openmbean.OpenMBeanParameterInfo;
import javax.management.openmbean.OpenMBeanParameterInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.jmx.ReadOnlyDynamicMBean;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemView;
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

    /** Filter operation name. */
    public static final String FILTER_OPERATION = "filter";

    /** Row id attribute name. */
    public static final String ID = "systemViewRowId";

    private static final Map<Class<?>, SimpleType<?>> CLASS_TO_SIMPLE_TYPE_MAP = new HashMap<>();

    static {
        registerClassToSimpleTypeRecords();
    }

    /** Maps classes to their SimpleType representation. */
    private static void registerClassToSimpleTypeRecords() {
        CLASS_TO_SIMPLE_TYPE_MAP.put(String.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(IgniteUuid.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(UUID.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Class.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(InetSocketAddress.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(BigDecimal.class, SimpleType.BIGDECIMAL);
        CLASS_TO_SIMPLE_TYPE_MAP.put(BigInteger.class, SimpleType.BIGINTEGER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Date.class, SimpleType.DATE);
        CLASS_TO_SIMPLE_TYPE_MAP.put(ObjectName.class, SimpleType.OBJECTNAME);
        CLASS_TO_SIMPLE_TYPE_MAP.put(boolean.class, SimpleType.BOOLEAN);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Boolean.class, SimpleType.BOOLEAN);
        CLASS_TO_SIMPLE_TYPE_MAP.put(byte.class, SimpleType.BYTE);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Byte.class, SimpleType.BYTE);
        CLASS_TO_SIMPLE_TYPE_MAP.put(short.class, SimpleType.SHORT);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Short.class, SimpleType.SHORT);
        CLASS_TO_SIMPLE_TYPE_MAP.put(int.class, SimpleType.INTEGER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Integer.class, SimpleType.INTEGER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(long.class, SimpleType.LONG);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Long.class, SimpleType.LONG);
        CLASS_TO_SIMPLE_TYPE_MAP.put(char.class, SimpleType.CHARACTER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Character.class, SimpleType.CHARACTER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(float.class, SimpleType.FLOAT);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Float.class, SimpleType.FLOAT);
        CLASS_TO_SIMPLE_TYPE_MAP.put(double.class, SimpleType.DOUBLE);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Double.class, SimpleType.DOUBLE);
    }

    /** System view to export. */
    private final SystemView<R> sysView;

    /** MBean info. */
    private final MBeanInfo info;

    /** Row type. */
    private final CompositeType rowType;

    /** System view type. */
    private final TabularType sysViewType;

    /** Filter field names. */
    private final String[] filterFields;

    /**
     * @param sysView System view to export.
     */
    public SystemViewMBean(SystemView<R> sysView) {
        this.sysView = sysView;

        int cnt = sysView.walker().count();

        String[] fields = new String[cnt+1];
        OpenType[] types = new OpenType[cnt+1];

        List<Integer> filterFieldIdxs = new ArrayList<>(cnt);

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                fields[idx] = name;
                types[idx] = CLASS_TO_SIMPLE_TYPE_MAP.getOrDefault(clazz, SimpleType.STRING);

                if (sysView.walker().filtrableAttributes().contains(name))
                    filterFieldIdxs.add(idx);
            }
        });

        fields[cnt] = ID;
        types[cnt] = SimpleType.INTEGER;

        try {
            rowType = new CompositeType(sysView.name(),
                sysView.description(),
                fields,
                fields,
                types);

            OpenMBeanOperationInfo[] operations = null;

            if (!filterFieldIdxs.isEmpty() && sysView instanceof FiltrableSystemView) {
                OpenMBeanParameterInfo[] params = new OpenMBeanParameterInfo[filterFieldIdxs.size()];

                filterFields = new String[filterFieldIdxs.size()];

                for (int i = 0; i < filterFieldIdxs.size(); i++) {
                    String fieldName = fields[filterFieldIdxs.get(i)];

                    filterFields[i] = fieldName;

                    params[i] = new OpenMBeanParameterInfoSupport(fieldName, fieldName, types[filterFieldIdxs.get(i)]);
                }

                OpenMBeanOperationInfo operation = new OpenMBeanOperationInfoSupport(FILTER_OPERATION,
                    "Filter view content", params, rowType, MBeanOperationInfo.INFO);

                operations = new OpenMBeanOperationInfo[] {operation};
            }
            else
                filterFields = null;

            info = new OpenMBeanInfoSupport(
                sysView.name(),
                sysView.description(),
                new OpenMBeanAttributeInfo[] {
                    new OpenMBeanAttributeInfoSupport(VIEWS, VIEWS, rowType, true, false, false)
                },
                null,
                operations,
                null
            );

            sysViewType = new TabularType(
                sysView.name(),
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

        if (attribute.equals(VIEWS))
            return viewContent(null);

        throw new IllegalArgumentException("Unknown attribute " + attribute);
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String actName, Object[] params,
        String[] signature) throws MBeanException, ReflectionException {
        if (FILTER_OPERATION.equals(actName)) {
            assert filterFields != null;
            assert filterFields.length >= params.length;

            Map<String, Object> filter = U.newHashMap(params.length);

            for (int i = 0; i < params.length; i++) {
                if (params[i] != null)
                    filter.put(filterFields[i], params[i]);
            }

            return viewContent(filter);
        }

        return super.invoke(actName, params, signature);
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        return info;
    }

    /**
     * Gets tabular data with system view content.
     */
    private TabularDataSupport viewContent(Map<String, Object> filter) {
        TabularDataSupport rows = new TabularDataSupport(sysViewType);

        AttributeToMapVisitor visitor = new AttributeToMapVisitor();

        try {
            int idx = 0;

            Iterable<R> iter = filter != null && sysView instanceof FiltrableSystemView ?
                () -> ((FiltrableSystemView<R>)sysView).iterator(filter) : sysView;

            for (R row : iter) {
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
