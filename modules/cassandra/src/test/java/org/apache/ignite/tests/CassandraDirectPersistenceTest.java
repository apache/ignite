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

package org.apache.ignite.tests;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.utils.CacheStoreHelper;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

/**
 * Unit tests for {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore} implementation of
 * {@link org.apache.ignite.cache.store.CacheStore} which allows to store Ignite cache data into Cassandra tables.
 */
public class CassandraDirectPersistenceTest {
    /** */
    private static final Logger LOGGER = Logger.getLogger(CassandraDirectPersistenceTest.class.getName());

    /** */
    @BeforeClass
    public static void setUpClass() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();

        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();

        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();

        LOGGER.info("Start tests execution");
    }

    /** */
    @AfterClass
    public static void tearDownClass() {
        try {
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();

            if (CassandraHelper.useEmbeddedCassandra()) {
                try {
                    CassandraHelper.stopEmbeddedCassandra();
                }
                catch (Throwable e) {
                    LOGGER.error("Failed to stop embedded Cassandra instance", e);
                }
            }
        }
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void primitiveStrategyTest() {
        CacheStore store1 = CacheStoreHelper.createCacheStore("longTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/primitive/persistence-settings-1.xml"),
            CassandraHelper.getAdminDataSrc());

        CacheStore store2 = CacheStoreHelper.createCacheStore("stringTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/primitive/persistence-settings-2.xml"),
            CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<Long, Long>> longEntries = TestsHelper.generateLongsEntries();
        Collection<CacheEntryImpl<String, String>> strEntries = TestsHelper.generateStringsEntries();

        Collection<Long> fakeLongKeys = TestsHelper.getKeys(longEntries);
        fakeLongKeys.add(-1L);
        fakeLongKeys.add(-2L);
        fakeLongKeys.add(-3L);
        fakeLongKeys.add(-4L);

        Collection<String> fakeStrKeys = TestsHelper.getKeys(strEntries);
        fakeStrKeys.add("-1");
        fakeStrKeys.add("-2");
        fakeStrKeys.add("-3");
        fakeStrKeys.add("-4");

        LOGGER.info("Running PRIMITIVE strategy write tests");

        LOGGER.info("Running single operation write tests");
        store1.write(longEntries.iterator().next());
        store2.write(strEntries.iterator().next());
        LOGGER.info("Single operation write tests passed");

        LOGGER.info("Running bulk operation write tests");
        store1.writeAll(longEntries);
        store2.writeAll(strEntries);
        LOGGER.info("Bulk operation write tests passed");

        LOGGER.info("PRIMITIVE strategy write tests passed");

        LOGGER.info("Running PRIMITIVE strategy read tests");

        LOGGER.info("Running single operation read tests");

        LOGGER.info("Running real keys read tests");

        Long longVal = (Long)store1.load(longEntries.iterator().next().getKey());
        if (!longEntries.iterator().next().getValue().equals(longVal))
            throw new RuntimeException("Long values was incorrectly deserialized from Cassandra");

        String strVal = (String)store2.load(strEntries.iterator().next().getKey());
        if (!strEntries.iterator().next().getValue().equals(strVal))
            throw new RuntimeException("String values was incorrectly deserialized from Cassandra");

        LOGGER.info("Running fake keys read tests");

        longVal = (Long)store1.load(-1L);
        if (longVal != null)
            throw new RuntimeException("Long value with fake key '-1' was found in Cassandra");

        strVal = (String)store2.load("-1");
        if (strVal != null)
            throw new RuntimeException("String value with fake key '-1' was found in Cassandra");

        LOGGER.info("Single operation read tests passed");

        LOGGER.info("Running bulk operation read tests");

        LOGGER.info("Running real keys read tests");

        Map longValues = store1.loadAll(TestsHelper.getKeys(longEntries));
        if (!TestsHelper.checkCollectionsEqual(longValues, longEntries))
            throw new RuntimeException("Long values was incorrectly deserialized from Cassandra");

        Map strValues = store2.loadAll(TestsHelper.getKeys(strEntries));
        if (!TestsHelper.checkCollectionsEqual(strValues, strEntries))
            throw new RuntimeException("String values was incorrectly deserialized from Cassandra");

        LOGGER.info("Running fake keys read tests");

        longValues = store1.loadAll(fakeLongKeys);
        if (!TestsHelper.checkCollectionsEqual(longValues, longEntries))
            throw new RuntimeException("Long values was incorrectly deserialized from Cassandra");

        strValues = store2.loadAll(fakeStrKeys);
        if (!TestsHelper.checkCollectionsEqual(strValues, strEntries))
            throw new RuntimeException("String values was incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk operation read tests passed");

        LOGGER.info("PRIMITIVE strategy read tests passed");

        LOGGER.info("Running PRIMITIVE strategy delete tests");

        LOGGER.info("Deleting real keys");

        store1.delete(longEntries.iterator().next().getKey());
        store1.deleteAll(TestsHelper.getKeys(longEntries));

        store2.delete(strEntries.iterator().next().getKey());
        store2.deleteAll(TestsHelper.getKeys(strEntries));

        LOGGER.info("Deleting fake keys");

        store1.delete(-1L);
        store2.delete("-1");

        store1.deleteAll(fakeLongKeys);
        store2.deleteAll(fakeStrKeys);

        LOGGER.info("PRIMITIVE strategy delete tests passed");
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void blobStrategyTest() {
        CacheStore store1 = CacheStoreHelper.createCacheStore("longTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/blob/persistence-settings-1.xml"),
            CassandraHelper.getAdminDataSrc());

        CacheStore store2 = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/blob/persistence-settings-2.xml"),
            CassandraHelper.getAdminDataSrc());

        CacheStore store3 = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/blob/persistence-settings-3.xml"),
            CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<Long, Long>> longEntries = TestsHelper.generateLongsEntries();
        Collection<CacheEntryImpl<Long, Person>> personEntries = TestsHelper.generateLongsPersonsEntries();

        LOGGER.info("Running BLOB strategy write tests");

        LOGGER.info("Running single operation write tests");
        store1.write(longEntries.iterator().next());
        store2.write(personEntries.iterator().next());
        store3.write(personEntries.iterator().next());
        LOGGER.info("Single operation write tests passed");

        LOGGER.info("Running bulk operation write tests");
        store1.writeAll(longEntries);
        store2.writeAll(personEntries);
        store3.writeAll(personEntries);
        LOGGER.info("Bulk operation write tests passed");

        LOGGER.info("BLOB strategy write tests passed");

        LOGGER.info("Running BLOB strategy read tests");

        LOGGER.info("Running single operation read tests");

        Long longVal = (Long)store1.load(longEntries.iterator().next().getKey());
        if (!longEntries.iterator().next().getValue().equals(longVal))
            throw new RuntimeException("Long values was incorrectly deserialized from Cassandra");

        Person personVal = (Person)store2.load(personEntries.iterator().next().getKey());
        if (!personEntries.iterator().next().getValue().equals(personVal))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        personVal = (Person)store3.load(personEntries.iterator().next().getKey());
        if (!personEntries.iterator().next().getValue().equals(personVal))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        LOGGER.info("Single operation read tests passed");

        LOGGER.info("Running bulk operation read tests");

        Map longValues = store1.loadAll(TestsHelper.getKeys(longEntries));
        if (!TestsHelper.checkCollectionsEqual(longValues, longEntries))
            throw new RuntimeException("Long values was incorrectly deserialized from Cassandra");

        Map personValues = store2.loadAll(TestsHelper.getKeys(personEntries));
        if (!TestsHelper.checkPersonCollectionsEqual(personValues, personEntries, false))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        personValues = store3.loadAll(TestsHelper.getKeys(personEntries));
        if (!TestsHelper.checkPersonCollectionsEqual(personValues, personEntries, false))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk operation read tests passed");

        LOGGER.info("BLOB strategy read tests passed");

        LOGGER.info("Running BLOB strategy delete tests");

        store1.delete(longEntries.iterator().next().getKey());
        store1.deleteAll(TestsHelper.getKeys(longEntries));

        store2.delete(personEntries.iterator().next().getKey());
        store2.deleteAll(TestsHelper.getKeys(personEntries));

        store3.delete(personEntries.iterator().next().getKey());
        store3.deleteAll(TestsHelper.getKeys(personEntries));

        LOGGER.info("BLOB strategy delete tests passed");
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void pojoStrategyTest() {
        CacheStore store1 = CacheStoreHelper.createCacheStore("longTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-1.xml"),
            CassandraHelper.getAdminDataSrc());

        CacheStore store2 = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-2.xml"),
            CassandraHelper.getAdminDataSrc());

        CacheStore store3 = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml"),
            CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<Long, Person>> entries1 = TestsHelper.generateLongsPersonsEntries();
        Collection<CacheEntryImpl<PersonId, Person>> entries2 = TestsHelper.generatePersonIdsPersonsEntries();
        Collection<CacheEntryImpl<PersonId, Person>> entries3 = TestsHelper.generatePersonIdsPersonsEntries();

        LOGGER.info("Running POJO strategy write tests");

        LOGGER.info("Running single operation write tests");
        store1.write(entries1.iterator().next());
        store2.write(entries2.iterator().next());
        store3.write(entries3.iterator().next());
        LOGGER.info("Single operation write tests passed");

        LOGGER.info("Running bulk operation write tests");
        store1.writeAll(entries1);
        store2.writeAll(entries2);
        store3.writeAll(entries3);
        LOGGER.info("Bulk operation write tests passed");

        LOGGER.info("POJO strategy write tests passed");

        LOGGER.info("Running POJO strategy read tests");

        LOGGER.info("Running single operation read tests");

        Person person = (Person)store1.load(entries1.iterator().next().getKey());
        if (!entries1.iterator().next().getValue().equalsPrimitiveFields(person))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        person = (Person)store2.load(entries2.iterator().next().getKey());
        if (!entries2.iterator().next().getValue().equalsPrimitiveFields(person))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        person = (Person)store3.load(entries3.iterator().next().getKey());
        if (!entries3.iterator().next().getValue().equals(person))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        LOGGER.info("Single operation read tests passed");

        LOGGER.info("Running bulk operation read tests");

        Map persons = store1.loadAll(TestsHelper.getKeys(entries1));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries1, true))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        persons = store2.loadAll(TestsHelper.getKeys(entries2));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries2, true))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        persons = store3.loadAll(TestsHelper.getKeys(entries3));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries3, false))
            throw new RuntimeException("Person values was incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk operation read tests passed");

        LOGGER.info("POJO strategy read tests passed");

        LOGGER.info("Running POJO strategy delete tests");

        store1.delete(entries1.iterator().next().getKey());
        store1.deleteAll(TestsHelper.getKeys(entries1));

        store2.delete(entries2.iterator().next().getKey());
        store2.deleteAll(TestsHelper.getKeys(entries2));

        store3.delete(entries3.iterator().next().getKey());
        store3.deleteAll(TestsHelper.getKeys(entries3));

        LOGGER.info("POJO strategy delete tests passed");
    }
}
