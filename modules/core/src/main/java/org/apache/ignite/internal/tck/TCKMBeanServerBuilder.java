/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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