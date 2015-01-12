/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.tck;

import com.sun.jmx.mbeanserver.*;

import javax.management.*;

/**
 *
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
        @Override
        public String getSpecificationVersion() {
            return delegate.getSpecificationVersion();
        }

        /** {@inheritDoc} */
        @Override
        public String getSpecificationVendor() {
            return delegate.getSpecificationVendor();
        }

        /** {@inheritDoc} */
        @Override
        public String getImplementationName() {
            return delegate.getImplementationName();
        }

        /** {@inheritDoc} */
        @Override
        public String getImplementationVersion() {
            return delegate.getImplementationVersion();
        }

        /** {@inheritDoc} */
        @Override
        public String getImplementationVendor() {
            return delegate.getImplementationVendor();
        }

        /** {@inheritDoc} */
        @Override
        public MBeanNotificationInfo[] getNotificationInfo() {
            return delegate.getNotificationInfo();
        }

        /** {@inheritDoc} */
        @Override
        public synchronized void addNotificationListener(NotificationListener listener,
                                                         NotificationFilter filter,
                                                         Object handback) throws
            IllegalArgumentException {
            delegate.addNotificationListener(listener, filter, handback);
        }

        /** {@inheritDoc} */
        @Override
        public synchronized void removeNotificationListener(NotificationListener
                                                                listener,
                                                            NotificationFilter
                                                                filter,
                                                            Object handback) throws
            ListenerNotFoundException {
            delegate.removeNotificationListener(listener, filter, handback);
        }

        /** {@inheritDoc} */
        @Override
        public synchronized void removeNotificationListener(NotificationListener
                                                                listener) throws
            ListenerNotFoundException {
            delegate.removeNotificationListener(listener);
        }

        /** {@inheritDoc} */
        @Override
        public void sendNotification(Notification notification) {
            delegate.sendNotification(notification);
        }

        /** {@inheritDoc} */
        @Override
        public synchronized String getMBeanServerId() {
            return System.getProperty("org.jsr107.tck.management.agentId");
        }
    }
}
