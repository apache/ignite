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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class Reproducer extends GridCommonAbstractTest {

    static final String NODE_1 = "node1";
    static final String NODE_2 = "node2";

    public static final String CREATE_TABLE = "CREATE TABLE PERSON (\n" +
        " FIRST_NAME VARCHAR,\n" +
        " LAST_NAME VARCHAR,\n" +
        " ADDRESS VARCHAR,\n" +
        " LANG VARCHAR,\n" +
        " BIRTH_DATE TIMESTAMP,\n" +
        " CONSTRAINT PK_PESON PRIMARY KEY (FIRST_NAME,LAST_NAME,ADDRESS,LANG)\n" +
        ") WITH \"key_type=PersonKeyType, CACHE_NAME=PersonCache, value_type=PersonValueType, AFFINITY_KEY=FIRST_NAME,template=PARTITIONED,backups=1\"";

    public static final String CREATE_INDEX = "create index PERSON_FIRST_NAME_IDX on  PERSON(FIRST_NAME)";

    public static final String QUERY = "select * from PERSON use index(PERSON_FIRST_NAME_IDX) \n" +
        "where \n" +
        "FIRST_NAME=?\n" +
        "and LAST_NAME=?\n" +
        "and ADDRESS=?\n" +
        "and LANG  = ? ";

    public static void main(String[] args) throws Exception {
        cleanPersistenceDir(); // TODO: Enable

        System.out.println(CREATE_TABLE);
        System.out.println(CREATE_INDEX);
        System.out.println(QUERY);

        Ignite server1 = Ignition.start(getCfg(NODE_1));
        Ignite server2 = Ignition.start(getCfg(NODE_2));
        Ignite client = Ignition.start(getCfg("client").setClientMode(true));
        client.cluster().active(true);

        createTable();
        fill();

        server2.close();

        createIndex();

        server2 = Ignition.start(getCfg(NODE_2));

        findRows(client);

        G.stopAll(true);
    }

    static IgniteConfiguration getCfg(String id) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        TcpDiscoverySpi discovery = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

        cfg.setConsistentId(id);
        cfg.setIgniteInstanceName(id);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        finder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        discovery.setIpFinder(finder);
        cfg.setDiscoverySpi(discovery);

        return cfg;
    }

    private static void createTable() throws SQLException {
        try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://localhost", new Properties())) {
            conn.createStatement().execute(CREATE_TABLE);
        }
    }

    private static void createIndex() throws SQLException {
        try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://localhost", new Properties())) {
            conn.createStatement().execute(CREATE_INDEX);
        }
    }

    private static void fill() throws SQLException {
        try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://localhost", new Properties())) {

            PreparedStatement st = conn.prepareStatement("insert into Person(LANG,FIRST_NAME,ADDRESS,LAST_NAME,BIRTH_DATE)\n" +
                "values(?,?,?,?,?)");

            for (int i = 0; i < 1; i++) {
                try {
                    String s = String.valueOf(i);
                    st.setString(1, s);
                    st.setString(2, s);
                    st.setString(3, s);
                    st.setString(4, s);
                    st.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
                    st.executeUpdate();
                }
                catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
        System.out.println("Tables with data are created.");
    }

    static void findRows(Ignite ignite) throws SQLException {
        AtomicInteger total = new AtomicInteger();
        AtomicInteger fails = new AtomicInteger();
        try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://localhost", new Properties())) {
            PreparedStatement st = conn.prepareStatement(QUERY);

            IgniteCache<BinaryObject, BinaryObject> cache = ignite.cache("PersonCache").withKeepBinary();
            cache.forEach(new Consumer<Cache.Entry<BinaryObject, BinaryObject>>() {
                @Override public void accept(Cache.Entry<BinaryObject, BinaryObject> entry) {
                    BinaryObject key = entry.getKey();
                    try {
                        st.setString(1, key.field("FIRST_NAME"));
                        st.setString(2, key.field("LAST_NAME"));
                        st.setString(3, key.field("ADDRESS"));
                        st.setString(4, key.field("LANG"));
                        ResultSet rs = st.executeQuery();
                        if (!rs.next()) {
                            System.out.println("!!!Unable to find row by key:" + key);
                            fails.incrementAndGet();
                        }

                        total.incrementAndGet();
                        rs.close();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            });

            st.close();

        }
        System.out.println("Finished");
        System.out.println("Total:"+total.get()+" fails:"+fails.get());
    }
}
