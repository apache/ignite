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
import org.apache.ignite.internal.processors.monitoring.sensor.Sensor;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;

/**
 *
 */
public class SensorGroupMBean implements DynamicMBean {
    public static final String GROUP_NAME = "groupName";
    public static final String SENSORS = "sensors";
    public static final String NAME = "name";
    public static final String VALUE = "value";

    private SensorGroup<?> grp;

    private MBeanInfo info;

    private CompositeType sensorType;

    private TabularType sensorListType;

    public SensorGroupMBean(SensorGroup<?> grp) {
        super();

        this.grp = grp;

        try {
            String typeName = SensorGroup.class.getName();

            sensorType = new CompositeType(typeName,
                "Sensors",
                new String[] {NAME, VALUE},
                new String[] {"Name of sensor", "Value of sensor"},
                new OpenType[] {SimpleType.STRING, SimpleType.STRING});

            sensorListType = new TabularType(typeName, "Sensors", sensorType, new String[] {NAME});

            OpenMBeanAttributeInfo[] attrs = new OpenMBeanAttributeInfo[] {
                new OpenMBeanAttributeInfoSupport(GROUP_NAME, "Group name", SimpleType.STRING, true, false, false),
                new OpenMBeanAttributeInfoSupport(SENSORS, SENSORS, sensorType, true, false, false),
            };

            this.info = new OpenMBeanInfoSupport(
                typeName,
                grp.getName().toString(),
                attrs,
                null,
                null,
                null);
        }
        catch (OpenDataException e) {
            throw new IgniteException(e);
        }
    }

    @Override public Object getAttribute(String attribute)
        throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attribute.equals(GROUP_NAME))
            return grp.getName();
        else if (attribute.equals(SENSORS)) {
            TabularDataSupport sensors = new TabularDataSupport(sensorListType);

            try {
                for (Sensor s : grp.getSensors()) {
                    Map<String, Object> sensor = new HashMap<>();

                    sensor.put(NAME, s.getName());
                    sensor.put(VALUE, s.toString());

                    sensors.put(new CompositeDataSupport(sensorType, sensor));
                }
            }
            catch (OpenDataException e) {
                throw new RuntimeException(e);
            }

            return sensors;
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
