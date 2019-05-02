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

import java.util.Iterator;
import java.util.Set;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.ignite.internal.processors.monitoring.sensor.Sensor;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;

/**
 *
 */
public class SensorGroupMBean implements DynamicMBean {
    private SensorGroup<?> grp;

    public SensorGroupMBean(SensorGroup<?> grp) {
        super();

        this.grp = grp;
    }

    @Override public Object getAttribute(String attribute)
        throws AttributeNotFoundException, MBeanException, ReflectionException {
        Sensor sensor = grp.findSensor(attribute);

        if (sensor == null)
            return null;

        return sensor.stringValue();
    }

    @Override public MBeanInfo getMBeanInfo() {
        Set<String> sensorNames = grp.names();
        Iterator<String> iter = sensorNames.iterator();

        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[sensorNames.size()];

        int sz = sensorNames.size();
        for (int i = 0; i < sz; i++) {
            String name = iter.next();

            attributes[i] = new MBeanAttributeInfo(name, "java.lang.String", name, true, false, false);
        }

        return new MBeanInfo(SensorGroup.class.getName(), grp.getName().toString(), attributes, null, null, null);
    }

    @Override public AttributeList getAttributes(String[] attributes) {
        return null;
    }

    @Override public void setAttribute(Attribute attribute)
        throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("setAttribute not supported.");
    }

    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes not supported.");
    }

    @Override public Object invoke(String actionName, Object[] params, String[] signature)
        throws MBeanException, ReflectionException {
        throw new UnsupportedOperationException("invoke not supported.");
    }
}
