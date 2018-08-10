package org.apache.ignite.internal.processors.cache;

import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Set;
import javax.cache.CacheException;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class CacheRollbackOnClientNodeReproducer extends GridCommonAbstractTest {

    public void test() throws Exception {
        Ignite serverNode = startGrid("server_node");

        Ignite clientNode = startGrid("client_node");

        mbSrv.cache("CACHE_FROM_CLIENT_NODE");
        mbSrv.node("client_node");

        try {
            clientNode.createCache("CACHE_FROM_CLIENT_NODE");

            fail();
        }
        catch (CacheException e) {
            e.printStackTrace();
        }

        awaitPartitionMapExchange();

        assertNull(((IgniteKernal)clientNode).context().cache().cache("CACHE_FROM_CLIENT_NODE"));

        assertNotNull(((IgniteKernal)serverNode).context().cache().cache("CACHE_FROM_CLIENT_NODE"));
    }

    private static FailureMBeanServer mbSrv;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration res = super.getConfiguration(igniteInstanceName);

        if (mbSrv == null)
            mbSrv = new FailureMBeanServer(res.getMBeanServer());

        res.setMBeanServer(mbSrv);

        if ("client_node".equals(igniteInstanceName))
            res.setClientMode(true);

        return res;
    }

    /** Failure MBean server. */
    private class FailureMBeanServer implements MBeanServer {
        /** */
        private final MBeanServer origin;

        /** Set of caches that must be failure. */
        private final Set<String> caches = new HashSet<>();

        /** Set of nodes that must be failure. */
        private final Set<String> nodes = new HashSet<>();

        /** */
        private FailureMBeanServer(MBeanServer origin) {
            this.origin = origin;
        }

        /** Add cache name to failure set. */
        void cache(String cache) {
            caches.add(cache);
        }

        /** Add node name to failure set. */
        void node(String node) {
            nodes.add(node);
        }

        /** Clear failure set of caches and set of nodes. */
        void clear() {
            caches.clear();
            nodes.clear();
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance registerMBean(Object obj, ObjectName name)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
            String node = name.getKeyProperty("igniteInstanceName");

            if (nodes.contains(node) && caches.contains(name.getKeyProperty("group")))
                throw new MBeanRegistrationException(new Exception("Simulate exception [node=" + node + ']'));

            return origin.registerMBean(obj, name);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException {
            return origin.createMBean(clsName, name);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name, ObjectName ldrName)
            throws ReflectionException, InstanceAlreadyExistsException,
            MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
            return origin.createMBean(clsName, name, ldrName);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name, Object[] params,
            String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
            MBeanException, NotCompliantMBeanException {
            return origin.createMBean(clsName, name, params, signature);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name, ObjectName ldrName,
            Object[] params, String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
            MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
            return origin.createMBean(clsName, name, ldrName, params, signature);
        }

        /** {@inheritDoc} */
        @Override public void unregisterMBean(
            ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
            origin.unregisterMBean(name);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
            return origin.getObjectInstance(name);
        }

        /** {@inheritDoc} */
        @Override public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp qry) {
            return origin.queryMBeans(name, qry);
        }

        /** {@inheritDoc} */
        @Override public Set<ObjectName> queryNames(ObjectName name, QueryExp qry) {
            return origin.queryNames(name, qry);
        }

        /** {@inheritDoc} */
        @Override public boolean isRegistered(ObjectName name) {
            return origin.isRegistered(name);
        }

        /** {@inheritDoc} */
        @Override public Integer getMBeanCount() {
            return origin.getMBeanCount();
        }

        /** {@inheritDoc} */
        @Override public Object getAttribute(ObjectName name, String attribute)
            throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
            return origin.getAttribute(name, attribute);
        }

        /** {@inheritDoc} */
        @Override public AttributeList getAttributes(ObjectName name,
            String[] attrs) throws InstanceNotFoundException, ReflectionException {
            return origin.getAttributes(name, attrs);
        }

        /** {@inheritDoc} */
        @Override public void setAttribute(ObjectName name,
            Attribute attribute) throws InstanceNotFoundException, AttributeNotFoundException,
            InvalidAttributeValueException, MBeanException, ReflectionException {
            origin.setAttribute(name, attribute);
        }

        /** {@inheritDoc} */
        @Override public AttributeList setAttributes(ObjectName name,
            AttributeList attrs) throws InstanceNotFoundException, ReflectionException {
            return origin.setAttributes(name, attrs);
        }

        /** {@inheritDoc} */
        @Override public Object invoke(ObjectName name, String operationName, Object[] params,
            String[] signature) throws InstanceNotFoundException, MBeanException, ReflectionException {
            return origin.invoke(name, operationName, params, signature);
        }

        /** {@inheritDoc} */
        @Override public String getDefaultDomain() {
            return origin.getDefaultDomain();
        }

        /** {@inheritDoc} */
        @Override public String[] getDomains() {
            return origin.getDomains();
        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(ObjectName name, NotificationListener lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException {
            origin.addNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(ObjectName name, ObjectName lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException {
            origin.addNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name,
            ObjectName lsnr) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name, ObjectName lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name,
            NotificationListener lsnr) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name, NotificationListener lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public MBeanInfo getMBeanInfo(
            ObjectName name) throws InstanceNotFoundException, IntrospectionException, ReflectionException {
            return origin.getMBeanInfo(name);
        }

        /** {@inheritDoc} */
        @Override public boolean isInstanceOf(ObjectName name, String clsName) throws InstanceNotFoundException {
            return origin.isInstanceOf(name, clsName);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName) throws ReflectionException, MBeanException {
            return origin.instantiate(clsName);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName,
            ObjectName ldrName) throws ReflectionException, MBeanException, InstanceNotFoundException {
            return origin.instantiate(clsName, ldrName);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName, Object[] params,
            String[] signature) throws ReflectionException, MBeanException {
            return origin.instantiate(clsName, params, signature);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName, ObjectName ldrName, Object[] params,
            String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
            return origin.instantiate(clsName, ldrName, params, signature);
        }

        /** {@inheritDoc} */
        @Override @Deprecated public ObjectInputStream deserialize(ObjectName name, byte[] data)
            throws OperationsException {
            return origin.deserialize(name, data);
        }

        /** {@inheritDoc} */
        @Override @Deprecated public ObjectInputStream deserialize(String clsName, byte[] data)
            throws OperationsException, ReflectionException {
            return origin.deserialize(clsName, data);
        }

        /** {@inheritDoc} */
        @Override @Deprecated public ObjectInputStream deserialize(String clsName, ObjectName ldrName, byte[] data)
            throws OperationsException, ReflectionException {
            return origin.deserialize(clsName, ldrName, data);
        }

        /** {@inheritDoc} */
        @Override public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
            return origin.getClassLoaderFor(mbeanName);
        }

        /** {@inheritDoc} */
        @Override public ClassLoader getClassLoader(ObjectName ldrName) throws InstanceNotFoundException {
            return origin.getClassLoader(ldrName);
        }

        /** {@inheritDoc} */
        @Override public ClassLoaderRepository getClassLoaderRepository() {
            return origin.getClassLoaderRepository();
        }
    }

}
