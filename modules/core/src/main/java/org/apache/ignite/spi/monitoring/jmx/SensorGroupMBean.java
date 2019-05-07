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
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.monitoring.sensor.Sensor;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;

/**
 *
 */
public class SensorGroupMBean implements DynamicMBean {
    private SensorGroup grp;

    public SensorGroupMBean(SensorGroup grp) {
        super();

        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(String attribute)
        throws AttributeNotFoundException, MBeanException, ReflectionException {
        Sensor sensor = grp.findSensor(attribute);

        if (sensor == null)
            return null;

        return sensor.stringValue();
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        Iterator<Sensor> iter = grp.getSensors().iterator();

        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[grp.getSensors().size()];

        int sz = attributes.length;
        for (int i = 0; i < sz; i++) {
            Sensor snsr = iter.next();

            attributes[i] = new MBeanAttributeInfo(
                snsr.getName(),
                "java.lang.String",
                snsr.getName(),
                true,
                false,
                false);
        }

        return new MBeanInfo(
            SensorGroup.class.getName(),
            grp.getName(),
            attributes,
            null,
            null,
            null);
    }

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();

            try {
                for (String attribute : attributes) {
                    Object val = getAttribute(attribute);

                    list.add(val);
                }
            }
            catch (ReflectionException | MBeanException | AttributeNotFoundException e) {
                throw new IgniteException(e);
            }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Attribute attribute)
        throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("setAttribute not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes not supported.");
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String actionName, Object[] params, String[] signature)
        throws MBeanException, ReflectionException {
        throw new UnsupportedOperationException("invoke not supported.");
    }
}
