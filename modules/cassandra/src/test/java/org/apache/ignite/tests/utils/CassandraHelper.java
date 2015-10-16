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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.ignite.cache.store.cassandra.utils.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.utils.session.pool.SessionPool;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Helper class providing bunch of utility methods to work with Cassandra
 */
public class CassandraHelper {
    private static final ResourceBundle CREDENTIALS = ResourceBundle.getBundle("org/apache/ignite/tests/cassandra/credentials");
    private static final ResourceBundle CONNECTION = ResourceBundle.getBundle("org/apache/ignite/tests/cassandra/connection");
    private static final ResourceBundle KEYSPACES = ResourceBundle.getBundle("org/apache/ignite/tests/cassandra/keyspaces");

    private static final ApplicationContext connectionContext = new ClassPathXmlApplicationContext("org/apache/ignite/tests/cassandra/connection-settings.xml");

    private static DataSource adminDataSource;
    private static DataSource regularDataSource;

    private static Cluster adminCluster;
    private static Cluster regularCluster;
    private static Session adminSession;
    private static Session regularSession;

    public static String getAdminUser() {
        return CREDENTIALS.getString("admin.user");
    }

    public static String getAdminPassword() {
        return CREDENTIALS.getString("admin.password");
    }

    public static String getRegularUser() {
        return CREDENTIALS.getString("regular.user");
    }

    public static String getRegularPassword() {
        return CREDENTIALS.getString("regular.password");
    }

    public static String[] getTestKeyspaces() {
        return KEYSPACES.getString("keyspaces").split(",");
    }

    public static String[] getContactPointsArray() {
        String[] points = CONNECTION.getString("contact.points").split(",");

        if (points.length == 0)
            throw new RuntimeException("No Cassandra contact points specified");

        for (int i = 0; i < points.length; i++)
            points[i] = points[i].trim();

        return points;
    }

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

    public static ResultSet executeWithAdminCredentials(String statement, Object... args) {
        if (args == null || args.length == 0)
            return adminSession().execute(statement);

        PreparedStatement ps = adminSession().prepare(statement);
        return adminSession().execute(ps.bind(args));
    }

    @SuppressWarnings("UnusedDeclaration")
    public static ResultSet executeWithRegularCredentials(String statement, Object... args) {
        if (args == null || args.length == 0)
            return regularSession().execute(statement);

        PreparedStatement ps = regularSession().prepare(statement);
        return regularSession().execute(ps.bind(args));
    }

    @SuppressWarnings("UnusedDeclaration")
    public static ResultSet executeWithAdminCredentials(Statement statement) {
        return adminSession().execute(statement);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static ResultSet executeWithRegularCredentials(Statement statement) {
        return regularSession().execute(statement);
    }

    public static synchronized DataSource getAdminDataSource() {
        if (adminDataSource != null)
            return adminDataSource;

        return adminDataSource = (DataSource)connectionContext.getBean("cassandraAdminDataSource");
    }

    @SuppressWarnings("UnusedDeclaration")
    public static synchronized DataSource getRegularDataSource() {
        if (regularDataSource != null)
            return regularDataSource;

        return regularDataSource = (DataSource)connectionContext.getBean("cassandraRegularDataSource");
    }

    public static void testAdminConnection() {
        try {
            adminSession();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to check admin connection to Cassandra", e);
        }
    }

    public static void testRegularConnection() {
        try {
            regularSession();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to check regular connection to Cassandra", e);
        }
    }

    public static synchronized void releaseCassandraResources() {
        try {
            if (adminSession != null && !adminSession.isClosed())
                adminSession.close();
        }
        catch (Throwable ignored) {
        }
        finally {
            adminSession = null;
        }

        try {
            if (adminCluster != null && !adminCluster.isClosed())
                adminCluster.close();
        }
        catch (Throwable ignored) {
        }
        finally {
            adminCluster = null;
        }

        try {
            if (regularSession != null && !regularSession.isClosed())
                regularSession.close();
        }
        catch (Throwable ignored) {
        }
        finally {
            regularSession = null;
        }

        try {
            if (regularCluster != null && !regularCluster.isClosed())
                regularCluster.close();
        }
        catch (Throwable ignored) {
        }
        finally {
            regularCluster = null;
        }

        SessionPool.release();
    }

    private static synchronized Session adminSession() {
        if (adminSession != null)
            return adminSession;

        try {
            Cluster.Builder builder = Cluster.builder();
            builder = builder.withCredentials(getAdminUser(), getAdminPassword());
            builder.addContactPoints(getContactPoints());
            builder.addContactPointsWithPorts(getContactPointsWithPorts());

            adminCluster = builder.build();
            return adminSession = adminCluster.connect();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create admin session to Cassandra database", e);
        }
    }

    private static synchronized Session regularSession() {
        if (regularSession != null)
            return regularSession;

        try {
            Cluster.Builder builder = Cluster.builder();
            builder = builder.withCredentials(getRegularUser(), getRegularPassword());
            builder.addContactPoints(getContactPoints());
            builder.addContactPointsWithPorts(getContactPointsWithPorts());

            regularCluster = builder.build();
            return regularSession = regularCluster.connect();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create regular session to Cassandra database", e);
        }
    }
}
