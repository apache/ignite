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

import java.io.ObjectInputStream;
import java.util.Set;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.loading.ClassLoaderRepository;

/**
 *
 */
class DummyMBeanServer implements MBeanServer {
    /** */
    public static final String[] DOMAINS = new String[0];

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInstance createMBean(String clsName, ObjectName name) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInstance createMBean(String clsName, ObjectName name, ObjectName ldrName) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInstance createMBean(String clsName, ObjectName name, Object[] params, String[] signature) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInstance createMBean(String clsName, ObjectName name, ObjectName ldrName, Object[] params, String[] signature) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInstance registerMBean(Object obj, ObjectName name) {
        return new ObjectInstance(name, obj.getClass().getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override public void unregisterMBean(ObjectName name) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInstance getObjectInstance(ObjectName name) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp qry) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Set<ObjectName> queryNames(ObjectName name, QueryExp qry) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean isRegistered(ObjectName name) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Integer getMBeanCount() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object getAttribute(ObjectName name, String attribute) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public AttributeList getAttributes(ObjectName name, String[] atts) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void setAttribute(ObjectName name, Attribute attribute) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public AttributeList setAttributes(ObjectName name, AttributeList atts) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String getDefaultDomain() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String[] getDomains() {
        return DOMAINS;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void addNotificationListener(ObjectName name, NotificationListener lsnr, NotificationFilter filter, Object handback) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public void addNotificationListener(ObjectName name, ObjectName lsnr, NotificationFilter filter, Object handback) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeNotificationListener(ObjectName name, ObjectName lsnr) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeNotificationListener(ObjectName name, ObjectName lsnr, NotificationFilter filter, Object handback) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeNotificationListener(ObjectName name, NotificationListener lsnr) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeNotificationListener(
        ObjectName name,
        NotificationListener lsnr,
        NotificationFilter filter,
        Object handback
    ) {

    }

    /**
     * {@inheritDoc}
     */
    @Override public MBeanInfo getMBeanInfo(ObjectName name) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean isInstanceOf(ObjectName name, String clsName) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object instantiate(String clsName) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object instantiate(String clsName, ObjectName ldrName) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object instantiate(String clsName, Object[] params, String[] signature) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object instantiate(String clsName, ObjectName ldrName, Object[] params, String[] signature) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInputStream deserialize(ObjectName name, byte[] data) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInputStream deserialize(String clsName, byte[] data) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ObjectInputStream deserialize(String clsName, ObjectName ldrName, byte[] data) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClassLoader getClassLoaderFor(ObjectName mbeanName) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClassLoader getClassLoader(ObjectName ldrName) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClassLoaderRepository getClassLoaderRepository() {
        return null;
    }
}
