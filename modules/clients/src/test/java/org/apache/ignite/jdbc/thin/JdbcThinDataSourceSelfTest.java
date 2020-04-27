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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import org.apache.ignite.IgniteJdbcThinDataSource;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * DataSource test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinDataSourceSelfTest extends JdbcThinAbstractSelfTest {
    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJndi() throws Exception {
        IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

        ids.setUrl("jdbc:ignite:thin://127.0.0.1");

        InitialContext ic = getInitialContext();

        ic.bind("ds/test", ids);

        IgniteJdbcThinDataSource ds = (IgniteJdbcThinDataSource)ic.lookup("ds/test");

        assertTrue("Cannot looking up DataSource from JNDI", ds != null);

        assertEquals(ids.getUrl(), ds.getUrl());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUrlCompose() throws Exception {
        IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

        ids.setAddresses("127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT);

        assertEquals("jdbc:ignite:thin://127.0.0.1:10800/PUBLIC", ids.getUrl());

        ids.setSchema("test");

        assertEquals("jdbc:ignite:thin://127.0.0.1:10800/test", ids.getUrl());
        assertEquals("jdbc:ignite:thin://127.0.0.1:10800/test", ids.getURL());

        ids.setAddresses("127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT + ".."
            + (ClientConnectorConfiguration.DFLT_PORT + 10), "myhost:10801..10802");

        ids.setSchema("test2");
        assertEquals("jdbc:ignite:thin://127.0.0.1:10800..10810,myhost:10801..10802/test2", ids.getURL());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResetUrl() throws Exception {
        IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

        ids.setUrl("jdbc:ignite:thin://127.0.0.1:10800/test?lazy=true");

        assertEquals("test", ids.getSchema());
        assertTrue(ids.isLazy());

        ids.setUrl("jdbc:ignite:thin://mydomain.org,localhost?collocated=true");

        assertNull(ids.getSchema());
        assertFalse(ids.isLazy());
        assertTrue(ids.isCollocated());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlHints() throws Exception {
        IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

        ids.setUrl("jdbc:ignite:thin://127.0.0.1");

        try (Connection conn = ids.getConnection()) {

            for (JdbcThinTcpIo io: ios(conn)) {
                assertFalse(io.connectionProperties().isAutoCloseServerCursor());
                assertFalse(io.connectionProperties().isCollocated());
                assertFalse(io.connectionProperties().isEnforceJoinOrder());
                assertFalse(io.connectionProperties().isLazy());
                assertFalse(io.connectionProperties().isDistributedJoins());
                assertFalse(io.connectionProperties().isReplicatedOnly());
            }
        }

        ids.setAutoCloseServerCursor(true);
        ids.setCollocated(true);
        ids.setEnforceJoinOrder(true);
        ids.setLazy(true);
        ids.setDistributedJoins(true);
        ids.setReplicatedOnly(true);

        try (Connection conn = ids.getConnection()) {

            for (JdbcThinTcpIo io: ios(conn)) {
                assertTrue(io.connectionProperties().isAutoCloseServerCursor());
                assertTrue(io.connectionProperties().isCollocated());
                assertTrue(io.connectionProperties().isEnforceJoinOrder());
                assertTrue(io.connectionProperties().isLazy());
                assertTrue(io.connectionProperties().isDistributedJoins());
                assertTrue(io.connectionProperties().isReplicatedOnly());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTcpNoDelay() throws Exception {
        IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

        ids.setUrl("jdbc:ignite:thin://127.0.0.1");

        try (Connection conn = ids.getConnection()) {

            for (JdbcThinTcpIo io: ios(conn))
                assertTrue(io.connectionProperties().isTcpNoDelay());
        }

        ids.setTcpNoDelay(false);

        try (Connection conn = ids.getConnection()) {

            for (JdbcThinTcpIo io: ios(conn))
                assertFalse(io.connectionProperties().isTcpNoDelay());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSocketBuffers() throws Exception {
        final IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

        ids.setUrl("jdbc:ignite:thin://127.0.0.1");
        ids.setSocketReceiveBuffer(111);
        ids.setSocketSendBuffer(111);

        try (Connection conn = ids.getConnection()) {

            for (JdbcThinTcpIo io: ios(conn)) {
                assertEquals(111, io.connectionProperties().getSocketReceiveBuffer());
                assertEquals(111, io.connectionProperties().getSocketSendBuffer());
            }
        }

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ids.setSocketReceiveBuffer(-1);

                ids.getConnection();

                return null;
            }
        }, SQLException.class, "Property cannot be lower than 0 [name=socketReceiveBuffer, value=-1]");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ids.setSocketReceiveBuffer(1024);
                ids.setSocketSendBuffer(-1);

                ids.getConnection();

                return null;
            }
        }, SQLException.class, "Property cannot be lower than 0 [name=socketSendBuffer, value=-1]");
    }

    /**
     * Initial context creation testing purposes
     * @return Initial context.
     * @throws Exception On error.
     */
    private InitialContext getInitialContext() throws Exception {
        Hashtable<String, String> env = new Hashtable<>();

        env.put(Context.INITIAL_CONTEXT_FACTORY, JndiContextMockFactory.class.getName());

        return new InitialContext(env);
    }

    /**
     *
     */
    public static class JndiContextMockFactory implements InitialContextFactory {
        /** {@inheritDoc} */
        @Override public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
            return new JndiMockContext();
        }
    }

    /**
     * Get client endpoints for connection.
     *
     * @param conn Connection.
     * @return Collection of endpoints.
     * @throws Exception If failed.
     */
    private static Collection<JdbcThinTcpIo> ios(Connection conn) throws Exception {
        JdbcThinConnection conn0 = conn.unwrap(JdbcThinConnection.class);

        Collection<JdbcThinTcpIo> ios = partitionAwareness ? ((Map<UUID, JdbcThinTcpIo>)
            GridTestUtils.getFieldValue(conn0, JdbcThinConnection.class, "ios")).values() :
            Collections.singleton(GridTestUtils.getFieldValue(conn0, JdbcThinConnection.class, "singleIo"));

        assert !ios.isEmpty();

        return ios;
    }

    /**
     *
     */
    public static class JndiMockContext implements Context {
        /** Objects map. */
        private Map<String, Object> map = new HashMap<String, Object>();

        /** {@inheritDoc} */
        @Override public Object lookup(Name name) throws NamingException {
            return lookup(name.get(0));
        }

        /** {@inheritDoc} */
        @Override public Object lookup(String name) throws NamingException {
            return map.get(name);
        }

        /** {@inheritDoc} */
        @Override public void bind(Name name, Object obj) throws NamingException {
            rebind(name.get(0), obj);
        }

        /** {@inheritDoc} */
        @Override public void bind(String name, Object obj) throws NamingException {
            rebind(name, obj);
        }

        /** {@inheritDoc} */
        @Override public void rebind(Name name, Object obj) throws NamingException {
            rebind(name.get(0), obj);
        }

        /** {@inheritDoc} */
        @Override public void rebind(String name, Object obj) throws NamingException {
            map.put(name, obj);
        }

        /** {@inheritDoc} */
        @Override public void unbind(Name name) throws NamingException {
            unbind(name.get(0));
        }

        /** {@inheritDoc} */
        @Override public void unbind(String name) throws NamingException {
            map.remove(name);
        }

        /** {@inheritDoc} */
        @Override public void rename(Name oldName, Name newName) throws NamingException {
            rename(oldName.get(0), newName.get(0));
        }

        /** {@inheritDoc} */
        @Override public void rename(String oldName, String newName) throws NamingException {
            map.put(newName, map.remove(oldName));
        }

        /** {@inheritDoc} */
        @Override public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void destroySubcontext(Name name) throws NamingException {
        }

        /** {@inheritDoc} */
        @Override public void destroySubcontext(String name) throws NamingException {
        }

        /** {@inheritDoc} */
        @Override public Context createSubcontext(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Context createSubcontext(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object lookupLink(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object lookupLink(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public NameParser getNameParser(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public NameParser getNameParser(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Name composeName(Name name, Name prefix) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String composeName(String name, String prefix) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object addToEnvironment(String propName, Object propVal) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object removeFromEnvironment(String propName) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Hashtable<?, ?> getEnvironment() throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void close() throws NamingException {
        }

        /** {@inheritDoc} */
        @Override public String getNameInNamespace() throws NamingException {
            return null;
        }
    }
}
