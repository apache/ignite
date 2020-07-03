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

package org.apache.ignite.tests.utils;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.atomic.AtomicInteger;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.session.pool.SessionPool;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Helper class providing bunch of utility methods to work with Cassandra
 */
public class CassandraHelper {
    /** */
    private static final ResourceBundle CREDENTIALS = ResourceBundle.getBundle("org/apache/ignite/tests/cassandra/credentials");

    /** */
    private static final ResourceBundle CONNECTION = ResourceBundle.getBundle("org/apache/ignite/tests/cassandra/connection");

    /** */
    private static final ResourceBundle KEYSPACES = ResourceBundle.getBundle("org/apache/ignite/tests/cassandra/keyspaces");

    /** */
    private static final String EMBEDDED_CASSANDRA_YAML = "org/apache/ignite/tests/cassandra/embedded-cassandra.yaml";

    /** */
    private static final ApplicationContext connectionContext = new ClassPathXmlApplicationContext("org/apache/ignite/tests/cassandra/connection-settings.xml");

    /** */
    private static DataSource adminDataSrc;

    /** */
    private static DataSource regularDataSrc;

    /** */
    private static Cluster adminCluster;

    /** */
    private static Cluster regularCluster;

    /** */
    private static Session adminSes;

    /** */
    private static Session regularSes;

    /** */
    private static CassandraLifeCycleBean embeddedCassandraBean;

    /** */
    public static String getAdminUser() {
        return CREDENTIALS.getString("admin.user");
    }

    /** */
    public static String getAdminPassword() {
        return CREDENTIALS.getString("admin.password");
    }

    /** */
    public static String getRegularUser() {
        return CREDENTIALS.getString("regular.user");
    }

    /** */
    public static String getRegularPassword() {
        return CREDENTIALS.getString("regular.password");
    }

    /** */
    public static String[] getTestKeyspaces() {
        return KEYSPACES.getString("keyspaces").split(",");
    }

    /** */
    private static AtomicInteger refCounter = new AtomicInteger(0);

    /** */
    public static String[] getContactPointsArray() {
        String[] points = CONNECTION.getString("contact.points").split(",");

        if (points.length == 0)
            throw new RuntimeException("No Cassandra contact points specified");

        for (int i = 0; i < points.length; i++)
            points[i] = points[i].trim();

        return points;
    }

    /** */
    public static List<InetAddress> getContactPoints() {
        String[] points = getContactPointsArray();

        List<InetAddress> contactPoints = new LinkedList<>();

        for (String point : points) {
            if (point.contains(":"))
                continue;

            try {
                contactPoints.add(InetAddress.getByName(point));
            }
            catch (Throwable e) {
                throw new IllegalArgumentException("Incorrect contact point '" + point +
                    "' specified for Cassandra cache storage", e);
            }
        }

        return contactPoints;
    }

    /** */
    public static List<InetSocketAddress> getContactPointsWithPorts() {
        String[] points = getContactPointsArray();

        List<InetSocketAddress> contactPoints = new LinkedList<>();

        for (String point : points) {
            if (!point.contains(":"))
                continue;

            String[] chunks = point.split(":");

            try {
                contactPoints.add(InetSocketAddress.createUnresolved(chunks[0].trim(), Integer.parseInt(chunks[1].trim())));
            }
            catch (Throwable e) {
                throw new IllegalArgumentException("Incorrect contact point '" + point +
                    "' specified for Cassandra cache storage", e);
            }
        }

        return contactPoints;
    }

    /**
     * Checks if embedded Cassandra should be used for unit tests
     * @return true if embedded Cassandra should be used
     */
    public static boolean useEmbeddedCassandra() {
        String[] contactPoints = getContactPointsArray();

        return contactPoints != null && contactPoints.length == 1 && contactPoints[0].trim().startsWith("127.0.0.1");
    }

    /** */
    public static void dropTestKeyspaces() {
        String[] keyspaces = getTestKeyspaces();

        for (String keyspace : keyspaces) {
            try {
                executeWithAdminCredentials("DROP KEYSPACE IF EXISTS " + keyspace + ";");
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to drop keyspace: " + keyspace, e);
            }
        }
    }

    /** */
    public static ResultSet executeWithAdminCredentials(String statement, Object... args) {
        if (args == null || args.length == 0)
            return adminSession().execute(statement);

        PreparedStatement ps = adminSession().prepare(statement);
        return adminSession().execute(ps.bind(args));
    }

    /** */
    public static ResultSet executeWithRegularCredentials(String statement, Object... args) {
        if (args == null || args.length == 0)
            return regularSession().execute(statement);

        PreparedStatement ps = regularSession().prepare(statement);
        return regularSession().execute(ps.bind(args));
    }

    /** */
    public static ResultSet executeWithAdminCredentials(Statement statement) {
        return adminSession().execute(statement);
    }

    /** */
    public static ResultSet executeWithRegularCredentials(Statement statement) {
        return regularSession().execute(statement);
    }

    /** */
    public static synchronized DataSource getAdminDataSrc() {
        if (adminDataSrc != null)
            return adminDataSrc;

        return adminDataSrc = (DataSource)connectionContext.getBean("cassandraAdminDataSource");
    }

    /** */
    public static synchronized DataSource getRegularDataSrc() {
        if (regularDataSrc != null)
            return regularDataSrc;

        return regularDataSrc = (DataSource)connectionContext.getBean("cassandraRegularDataSource");
    }

    /** */
    public static void testAdminConnection() {
        try {
            adminSession();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to check admin connection to Cassandra", e);
        }
    }

    /** */
    public static void testRegularConnection() {
        try {
            regularSession();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to check regular connection to Cassandra", e);
        }
    }

    /** */
    public static synchronized void releaseCassandraResources() {
        try {
            if (adminSes != null && !adminSes.isClosed())
                U.closeQuiet(adminSes);
        }
        finally {
            adminSes = null;
        }

        try {
            if (adminCluster != null && !adminCluster.isClosed())
                U.closeQuiet(adminCluster);
        }
        finally {
            adminCluster = null;
        }

        try {
            if (regularSes != null && !regularSes.isClosed())
                U.closeQuiet(regularSes);
        }
        finally {
            regularSes = null;
        }

        try {
            if (regularCluster != null && !regularCluster.isClosed())
                U.closeQuiet(regularCluster);
        }
        finally {
            regularCluster = null;
        }

        SessionPool.release();
    }

    /** */
    private static synchronized Session adminSession() {
        if (adminSes != null)
            return adminSes;

        try {
            Cluster.Builder builder = Cluster.builder();
            builder = builder.withCredentials(getAdminUser(), getAdminPassword());
            builder.addContactPoints(getContactPoints());
            builder.addContactPointsWithPorts(getContactPointsWithPorts());

            adminCluster = builder.build();
            return adminSes = adminCluster.connect();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create admin session to Cassandra database", e);
        }
    }

    /** */
    private static synchronized Session regularSession() {
        if (regularSes != null)
            return regularSes;

        try {
            Cluster.Builder builder = Cluster.builder();
            builder = builder.withCredentials(getRegularUser(), getRegularPassword());
            builder.addContactPoints(getContactPoints());
            builder.addContactPointsWithPorts(getContactPointsWithPorts());

            regularCluster = builder.build();
            return regularSes = regularCluster.connect();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create regular session to Cassandra database", e);
        }
    }

    /**
     * Note that setting of cassandra.storagedir property is expected.
     */
    public static void startEmbeddedCassandra(Logger log) {
        if (refCounter.getAndIncrement() > 0)
            return;

        ClassLoader clsLdr = CassandraHelper.class.getClassLoader();
        URL url = clsLdr.getResource(EMBEDDED_CASSANDRA_YAML);

        embeddedCassandraBean = new CassandraLifeCycleBean();
        embeddedCassandraBean.setCassandraConfigFile(url.getFile());

        try {
            Field logField = CassandraLifeCycleBean.class.getDeclaredField("log");
            logField.setAccessible(true);
            logField.set(embeddedCassandraBean, new Log4JLogger(log));
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to initialize logger for CassandraLifeCycleBean", e);
        }

        embeddedCassandraBean.onLifecycleEvent(LifecycleEventType.BEFORE_NODE_START);
    }

    /** */
    public static void stopEmbeddedCassandra() {
        if (refCounter.decrementAndGet() > 0)
            return;

        if (embeddedCassandraBean != null)
            embeddedCassandraBean.onLifecycleEvent(LifecycleEventType.BEFORE_NODE_STOP);
    }
}
