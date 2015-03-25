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

package org.apache.ignite.examples.datagrid.store;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.examples.datagrid.store.dummy.*;
import org.apache.ignite.examples.datagrid.store.model.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import javax.cache.configuration.*;
import java.sql.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 * Starts up an empty node with example cache configuration.
 */
public class CacheNodeWithStoreStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignition.start(configure());
    }

    /**
     * Configure ignite.
     *
     * @return Ignite configuration.
     * @throws IgniteException If failed.
     */
    public static IgniteConfiguration configure() throws IgniteException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        // Discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>();

        // Set atomicity as transaction, since we are showing transactions in example.
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        // Set query indexing enabled for use query in example.
        cacheCfg.setQueryIndexEnabled(true);

        CacheStore<Long, Person> store;

        // Uncomment other cache stores to try them.
        store = new CacheDummyPersonStore();
        // store = new CacheJdbcPersonStore();
        // store = new CacheHibernatePersonStore();

        // Uncomment two lines for try CacheJdbcPojoStore.
        // store = new CacheJdbcPojoPersonStore();
        // cacheCfg.setTypeMetadata(typeMetadata());

        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory<>(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @return Type mapping description.
     */
    private static Collection<CacheTypeMetadata> typeMetadata() {
        CacheTypeMetadata tm = new CacheTypeMetadata();

        tm.setDatabaseTable("PERSONS");

        tm.setKeyType("java.lang.Long");
        tm.setValueType("org.apache.ignite.examples.datagrid.store.model.Person");

        tm.setKeyFields(F.asList(new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", Long.class)));

        tm.setValueFields(F.asList(
            new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", long.class),
            new CacheTypeFieldMetadata("FIRST_NAME", Types.VARCHAR, "firstName", String.class),
            new CacheTypeFieldMetadata("LAST_NAME", Types.VARCHAR, "lastName", String.class)
        ));

        return F.asList(tm);
    }
}
