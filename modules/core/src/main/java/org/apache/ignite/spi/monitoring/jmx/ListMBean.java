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

package org.apache.ignite.spi.monitoring.jmx;

import java.util.HashMap;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import javax.management.openmbean.OpenMBeanAttributeInfo;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.TabularDataSupport;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.monitoring.lists.ComputeTaskMonitoringInfo;
import org.apache.ignite.internal.processors.monitoring.lists.ListRow;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringList;
import org.apache.ignite.services.ServiceMonitoringInfo;
import org.apache.ignite.spi.monitoring.jmx.lists.ComputeTaskInfoRowListHelper;
import org.apache.ignite.spi.monitoring.jmx.lists.ListRowMBeanHelper;
import org.apache.ignite.spi.monitoring.jmx.lists.ServiceInfoRowListHelper;

/**
 *
 */
public class ListMBean<Id, Row> implements DynamicMBean {
    public static final String LIST_NAME = "listName";
    public static final String LIST = "list";

    private static Map<Class, ListRowMBeanHelper> ROW_HELPERS = new HashMap<>();

    static {
        ROW_HELPERS.put(ComputeTaskMonitoringInfo.class, new ComputeTaskInfoRowListHelper());
        ROW_HELPERS.put(ServiceMonitoringInfo.class, new ServiceInfoRowListHelper());
    }

    private MonitoringList<Id, Row> list;

    private ListRowMBeanHelper<Id, Row> listHelper;

    private MBeanInfo info;

    public ListMBean(MonitoringList<Id, Row> list) {
        super();

        assert ROW_HELPERS.containsKey(list.rowClass());

        this.list = list;
        this.listHelper = ROW_HELPERS.get(list.rowClass());


        this.info = new OpenMBeanInfoSupport(
            list.rowClass().getName(),
            list.getName().toString(),
            new OpenMBeanAttributeInfo[] {
                new OpenMBeanAttributeInfoSupport(LIST, LIST, listHelper.rowType(), true, false, false)
            },
            null,
            null,
            null
        );
    }

    @Override public Object getAttribute(String attribute)
        throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attribute.equals(LIST)) {
            TabularDataSupport rows = new TabularDataSupport(listHelper.listType());

            for (ListRow<Id, Row> row : list)
                rows.put(listHelper.row(row));

            return rows;
        }

        throw new IllegalArgumentException("Unknown attribute " + attribute);
    }

    @Override public void setAttribute(Attribute attribute)
        throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("setAttribute is not supported");
    }

    @Override public AttributeList getAttributes(String[] attributes) {
        try {
            AttributeList res = new AttributeList();

            for (String name : attributes) {
                Object val = getAttribute(name);

                res.add(new Attribute(name, val));
            }

            return res;
        }
        catch (MBeanException | AttributeNotFoundException | ReflectionException e) {
            throw new IgniteException(e);
        }
    }

    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes is not supported");
    }

    @Override public Object invoke(String actionName, Object[] params,
        String[] signature) throws MBeanException, ReflectionException {
        throw new UnsupportedOperationException("invoke is not supported");
    }

    @Override public MBeanInfo getMBeanInfo() {
        return info;
    }
}
