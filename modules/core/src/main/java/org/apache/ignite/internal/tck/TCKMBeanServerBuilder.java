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

package org.apache.ignite.internal.tck;

import com.sun.jmx.mbeanserver.JmxMBeanServer;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

/**
 * This class is needed for JCache TCK tests.
 */
public class TCKMBeanServerBuilder extends MBeanServerBuilder {
    /** {@inheritDoc} */
    @Override public MBeanServer newMBeanServer(String dfltDomain, MBeanServer outer, MBeanServerDelegate delegate) {
        MBeanServerDelegate decoratingDelegate = new ServerDelegate(delegate);
        return JmxMBeanServer.newMBeanServer(dfltDomain, outer,
            decoratingDelegate, false);
    }

    /**
     *
     */
    private static class ServerDelegate extends MBeanServerDelegate {
        /** */
        private final MBeanServerDelegate delegate;

        /**
         * Constructor
         *
         * @param delegate the provided delegate
         */
        ServerDelegate(MBeanServerDelegate delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public String getSpecificationName() {
            return delegate.getSpecificationName();
        }

        /** {@inheritDoc} */
        @Override public String getSpecificationVersion() {
            return delegate.getSpecificationVersion();
        }

        /** {@inheritDoc} */
        @Override public String getSpecificationVendor() {
            return delegate.getSpecificationVendor();
        }

        /** {@inheritDoc} */
        @Override public String getImplementationName() {
            return delegate.getImplementationName();
        }

        /** {@inheritDoc} */
        @Override public String getImplementationVersion() {
            return delegate.getImplementationVersion();
        }

        /** {@inheritDoc} */
        @Override public String getImplementationVendor() {
            return delegate.getImplementationVendor();
        }

        /** {@inheritDoc} */
        @Override public MBeanNotificationInfo[] getNotificationInfo() {
            return delegate.getNotificationInfo();
        }

        /** {@inheritDoc} */
        @Override public synchronized void addNotificationListener(NotificationListener lsnr,
            NotificationFilter filter,
            Object handback) throws
            IllegalArgumentException {
            delegate.addNotificationListener(lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public synchronized void removeNotificationListener(NotificationListener lsnr,
            NotificationFilter filter,
            Object handback) throws
            ListenerNotFoundException {
            delegate.removeNotificationListener(lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public synchronized void removeNotificationListener(NotificationListener lsnr)
            throws ListenerNotFoundException {
            delegate.removeNotificationListener(lsnr);
        }

        /** {@inheritDoc} */
        @Override public void sendNotification(Notification notification) {
            delegate.sendNotification(notification);
        }

        /** {@inheritDoc} */
        @Override public synchronized String getMBeanServerId() {
            return System.getProperty("org.jsr107.tck.management.agentId");
        }
    }
}