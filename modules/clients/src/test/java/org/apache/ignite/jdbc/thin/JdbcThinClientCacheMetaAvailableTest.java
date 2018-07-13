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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * JDBC driver reconnect test with multiple addresses.
 */
public class JdbcThinClientCacheMetaAvailableTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SERVER_PORT = 10800;

    /** */
    private static final int CLIENT_PORT = 10801;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        boolean client = name.contains("client");

        cfg.setClientMode(client);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setPort(client ? CLIENT_PORT : SERVER_PORT));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientUsesAfterRestart() throws Exception {
        startGrid("server");
        IgniteEx client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(client, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        stopGrid("client");

        client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(client, "");
             ResultSet meta = conn.getMetaData().getTables(null, "PUBLIC", null, null)) {
            assertTrue(meta.next());
            assertEquals("CITY", meta.getString("TABLE_NAME"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientDropsAfterRestart() throws Exception {
        startGrid("server");
        IgniteEx client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(client, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        stopGrid("client");

        client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(client, "");
             PreparedStatement st = conn.prepareStatement("DROP TABLE City;")) {
            st.execute();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerCreatesClientUses() throws Exception {
        IgniteEx server = (IgniteEx)startGrid("server");
        IgniteEx client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(server, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        try (Connection conn = connect(client, "");
             ResultSet meta = conn.getMetaData().getTables(null, "PUBLIC", null, null)) {
            assertTrue(meta.next());
            assertEquals("CITY", meta.getString("TABLE_NAME"));
        }
    }
    /**
     * @throws Exception If failed.
     */
    public void testServerCreatesClientDrops() throws Exception {
        IgniteEx server = (IgniteEx)startGrid("server");
        IgniteEx client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(server, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        try (Connection conn = connect(client, "");
             PreparedStatement st = conn.prepareStatement("DROP TABLE City;")) {
            st.execute();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerCreatesClientLists() throws Exception {
        IgniteEx server = (IgniteEx)startGrid("server");
        IgniteEx client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(server, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        assertEquals("SQL_PUBLIC_CITY", client.cacheNames().iterator().next());
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerCreatesClientUsesAfterSelect() throws Exception {
        IgniteEx server = (IgniteEx)startGrid("server");
        IgniteEx client = (IgniteEx)startGrid("client");

        try (Connection conn = connect(server, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        try (Connection conn = connect(client, "");
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM city;");
             ResultSet rs = ps.executeQuery();
             ResultSet meta = conn.getMetaData().getTables(null, "PUBLIC", null, null)) {
            assertTrue(meta.next());
            assertEquals("CITY", meta.getString("TABLE_NAME"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleServer() throws Exception {
        IgniteEx server = (IgniteEx)startGrid("server");

        try (Connection conn = connect(server, "");
             PreparedStatement st = conn.prepareStatement(
                 "CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR)WITH \"template=replicated\";")) {
            st.execute();
        }

        try (Connection conn = connect(server, "");
            ResultSet meta = conn.getMetaData().getTables(null, "PUBLIC", null, null)) {
            assertTrue(meta.next());
            assertEquals("CITY", meta.getString("TABLE_NAME"));
        }
    }
}
