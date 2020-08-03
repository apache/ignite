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

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.IgniteException;

/**
 * Base class for read only {@link DynamicMBean} implementations.
 */
public abstract class ReadOnlyDynamicMBean implements DynamicMBean {
    /** {@inheritDoc} */
    @Override public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("setAttribute is not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes is not supported.");
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String actionName, Object[] params, String[] signature)
        throws MBeanException, ReflectionException {
        try {
            if ("getAttribute".equals(actionName))
                return getAttribute((String)params[0]);
            else if ("invoke".equals(actionName))
                return invoke((String)params[0], (Object[])params[1], (String[])params[2]);
        }
        catch (AttributeNotFoundException e) {
            throw new MBeanException(e);
        }

        throw new UnsupportedOperationException("invoke is not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();

        try {
            for (String attribute : attributes) {
                Object val = getAttribute(attribute);

                list.add(val);
            }

            return list;
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            throw new IgniteException(e);
        }
    }
}
