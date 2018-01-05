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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for native API - JDBC interoperability.
 */
@SuppressWarnings("unused")
public class JdbcInteroperabilitySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "City";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test interoperability.
     *
     * @throws Exception If failed.
     */
    public void testInteroperability() throws Exception {
        // Start a node.
        Ignite srv = startGrid(config("srv", false));

        // Create table through SQL.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS City;");

                stmt.executeUpdate(
                    "CREATE TABLE City (\n" +
                    "  ID INT(11),\n" +
                    "  Name CHAR(35),\n" +
                    "  \"CountryCode\" CHAR(3),\n" +
                    "  District CHAR(20),\n" +
                    "  Population INT(11),\n" +
                    "  PRIMARY KEY (ID, \"CountryCode\")\n" +
                    ") WITH \"" +
                        "template=partitioned, " +
                        "backups=1, " +
                        "affinityKey='CountryCode', " +
                        "CACHE_NAME=" + CACHE_NAME + ", " +
                        "KEY_TYPE=" + CityKey.class.getName() + ", " +
                        "VALUE_TYPE=" + City.class.getName() + "\";"
                );

                stmt.executeUpdate(
                    "INSERT INTO City(ID, Name, \"CountryCode\", District, Population) " +
                    "VALUES (5,'Amsterdam','NLD','Noord-Holland',731200);"
                );

                stmt.executeUpdate(
                    "INSERT INTO City(ID, Name, \"CountryCode\", District, Population) " +
                    "VALUES (3068,'Berlin','DEU','Berliini',3386667);"
                );
            }
        }

        read(srv);

        Ignite cli = startGrid(config("client", true));

        migrateBinary(cli);
        migrateBinary(cli);
    }

    /**
     * Read data through native API.
     *
     * @param ignite Ignite.
     * @throws Exception If failed.
     */
    private void read(Ignite ignite) throws Exception {
        IgniteCache<CityKey, City> cache = ignite.cache(CACHE_NAME);

        CityKey key = new CityKey(5, "NLD");
        City city = cache.get(key); //BUG_1 !!!

        System.out.println(">> Getting Amsterdam Record:");
        System.out.println(city);

        System.out.println(">> Updating Amsterdam record:");
        city.population = city.population - 10_000;

        cache.put(key, city);

        System.out.println(cache.get(key));
    }

    /**
     * Migrate people between cities.
     *
     * @param ignite Node.
     * @throws Exception If failed.
     */
    private void migrateBinary(Ignite ignite) throws Exception {
        IgniteCache<BinaryObject, BinaryObject> cache = ignite.cache(CACHE_NAME).withKeepBinary();

        IgniteTransactions igniteTx = ignite.transactions();

        BinaryObject amKey = ignite.binary().builder(CityKey.class.getName())
            .setField("ID", 5).setField("CountryCode", "NLD").build();

        BinaryObject berKey = ignite.binary().builder(CityKey.class.getName())
            .setField("ID", 3068).setField("CountryCode", "DEU").build();

        System.out.println();
        System.out.println(">> Moving people between Amsterdam and Berlin");

        try (Transaction tx = igniteTx.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.invoke(amKey, new CityEntryProcessor(true, 100_000));
            cache.invoke(berKey, new CityEntryProcessor(false, 100_000));
        }

        BinaryObject am = cache.get(amKey);
        BinaryObject ber = cache.get(berKey);

        System.out.println(">> After update:");
        System.out.println(am);
        System.out.println(ber);
    }

    /**
     * Create configuration.
     *
     * @param name Name.
     * @param client Client flag.
     * @return Configuration.
     */
    private static IgniteConfiguration config(String name, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);
        cfg.setClientMode(client);
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoveryVmIpFinder ipFinder =
            new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList("127.0.0.1:47500..47501"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * City key.
     */
    private static class CityKey {
        /** */
        private int ID;

        /** */
        @AffinityKeyMapped
        private String CountryCode;

        /**
         * Constructor.
         *
         * @param id City ID.
         * @param CountryCode Country code (affinity key).
         */
        public CityKey(int id, String CountryCode) {
            this.ID = id;
            this.CountryCode = CountryCode;
        }
    }

    /**
     * City.
     */
    private static class City {
        /** */
        private String name;

        /** */
        private String district;

        /** */
        private int population;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param district District.
         * @param population Population.
         */
        public City(String name, String district, int population) {
            this.name = name;
            this.district = district;
            this.population = population;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "City{" +
                "name='" + name + '\'' +
                ", district='" + district + '\'' +
                ", population=" + population +
                '}';
        }
    }

    /**
     * Using collocated processing approach to update an entry directly on the server side w/o deserialization.
     * */
    private static class CityEntryProcessor implements EntryProcessor<BinaryObject, BinaryObject, Object> {
        /** Whether delta should be added or subtracted. */
        private boolean increase;

        /** Delta. */
        private int delta;

        /**
         * Constructor.
         *
         * @param increase Whether delta should be added or subtracted.
         * @param delta Delta.
         */
        public CityEntryProcessor(boolean increase, int delta) {
            this.increase = increase;
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<BinaryObject, BinaryObject> entry, Object... arguments)
            throws EntryProcessorException {
            BinaryObject val = entry.getValue();

            System.out.println(">> Before update:");
            System.out.println(val);

            if (increase)
                val = val.toBuilder().setField("Population", (int)val.field("Population") + delta).build();
            else
                val = val.toBuilder().setField("Population", (int)val.field("Population") - delta).build();

            entry.setValue(val);

            return entry;
        }
    }
}
