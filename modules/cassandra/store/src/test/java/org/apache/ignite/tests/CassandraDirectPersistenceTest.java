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

import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.tests.pojos.*;
import org.apache.ignite.tests.utils.*;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
            } catch (Throwable e) {
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
        } finally {
            CassandraHelper.releaseCassandraResources();

            if (CassandraHelper.useEmbeddedCassandra()) {
                try {
                    CassandraHelper.stopEmbeddedCassandra();
                } catch (Throwable e) {
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

        CacheStore store3 = CacheStoreHelper.createCacheStore("longStringTypes",
                new ClassPathResource("org/apache/ignite/tests/persistence/primitive/persistence-settings-3-handler.xml"),
                CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<Long, Long>> longEntries = TestsHelper.generateLongsEntries();
        Collection<CacheEntryImpl<String, String>> strEntries = TestsHelper.generateStringsEntries();
        Collection<CacheEntryImpl<Long, String>> longStrEntries = TestsHelper.generateLongStringEntries();


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

        Collection<Long> fakeLongStrKeys = TestsHelper.getKeys(longStrEntries);
        fakeLongStrKeys.add(-1L);
        fakeLongStrKeys.add(-2L);
        fakeLongStrKeys.add(-3L);
        fakeLongStrKeys.add(-4L);

        LOGGER.info("Running PRIMITIVE strategy write tests");

        LOGGER.info("Running single write operation tests");
        store1.write(longEntries.iterator().next());
        store2.write(strEntries.iterator().next());
        store3.write(longStrEntries.iterator().next());
        LOGGER.info("Single write operation tests passed");

        LOGGER.info("Running bulk write operation tests");
        store1.writeAll(longEntries);
        store2.writeAll(strEntries);
        store3.writeAll(longStrEntries);
        LOGGER.info("Bulk write operation tests passed");

        LOGGER.info("PRIMITIVE strategy write tests passed");

        LOGGER.info("Running PRIMITIVE strategy read tests");

        LOGGER.info("Running single read operation tests");

        LOGGER.info("Running real keys read tests");

        Long longVal = (Long) store1.load(longEntries.iterator().next().getKey());
        if (!longEntries.iterator().next().getValue().equals(longVal))
            throw new RuntimeException("Long values were incorrectly deserialized from Cassandra");

        String strVal = (String) store2.load(strEntries.iterator().next().getKey());
        if (!strEntries.iterator().next().getValue().equals(strVal))
            throw new RuntimeException("String values were incorrectly deserialized from Cassandra");

        String longStrVal = (String) store3.load(longStrEntries.iterator().next().getKey());
        if (!longStrEntries.iterator().next().getValue().equals(longStrVal))
            throw new RuntimeException("LongString values were incorrectly deserialized from Cassandra");

        LOGGER.info("Running fake keys read tests");

        longVal = (Long) store1.load(-1L);
        if (longVal != null)
            throw new RuntimeException("Long value with fake key '-1' was found in Cassandra");

        strVal = (String) store2.load("-1");
        if (strVal != null)
            throw new RuntimeException("String value with fake key '-1' was found in Cassandra");

        longStrVal = (String) store3.load(-1L);
        if (longStrVal != null)
            throw new RuntimeException("LongString value with fake key '-1' was found in Cassandra");

        LOGGER.info("Single read operation tests passed");

        LOGGER.info("Running bulk read operation tests");

        LOGGER.info("Running real keys read tests");

        Map longValues = store1.loadAll(TestsHelper.getKeys(longEntries));
        if (!TestsHelper.checkCollectionsEqual(longValues, longEntries))
            throw new RuntimeException("Long values were incorrectly deserialized from Cassandra");

        Map strValues = store2.loadAll(TestsHelper.getKeys(strEntries));
        if (!TestsHelper.checkCollectionsEqual(strValues, strEntries))
            throw new RuntimeException("String values were incorrectly deserialized from Cassandra");

        Map longStrValues = store3.loadAll(TestsHelper.getKeys(longStrEntries));
        if (!TestsHelper.checkCollectionsEqual(longStrValues, longStrEntries))
            throw new RuntimeException("LongString values were incorrectly deserialized from Cassandra");

        LOGGER.info("Running fake keys read tests");

        longValues = store1.loadAll(fakeLongKeys);
        if (!TestsHelper.checkCollectionsEqual(longValues, longEntries))
            throw new RuntimeException("Long values were incorrectly deserialized from Cassandra");

        strValues = store2.loadAll(fakeStrKeys);
        if (!TestsHelper.checkCollectionsEqual(strValues, strEntries))
            throw new RuntimeException("String values were incorrectly deserialized from Cassandra");

        longStrValues = store3.loadAll(fakeLongStrKeys);
        if (!TestsHelper.checkCollectionsEqual(longStrValues, longStrEntries))
            throw new RuntimeException("LongString values were incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk read operation tests passed");

        LOGGER.info("PRIMITIVE strategy read tests passed");

        LOGGER.info("Running PRIMITIVE strategy delete tests");

        LOGGER.info("Deleting real keys");

        store1.delete(longEntries.iterator().next().getKey());
        store1.deleteAll(TestsHelper.getKeys(longEntries));

        store2.delete(strEntries.iterator().next().getKey());
        store2.deleteAll(TestsHelper.getKeys(strEntries));

        store3.delete(longStrEntries.iterator().next().getKey());
        store3.deleteAll(TestsHelper.getKeys(longStrEntries));

        LOGGER.info("Deleting fake keys");

        store1.delete(-1L);
        store2.delete("-1");
        store3.delete(-1L);

        store1.deleteAll(fakeLongKeys);
        store2.deleteAll(fakeStrKeys);
        store3.deleteAll(fakeLongStrKeys);

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

        LOGGER.info("Running single write operation tests");
        store1.write(longEntries.iterator().next());
        store2.write(personEntries.iterator().next());
        store3.write(personEntries.iterator().next());
        LOGGER.info("Single write operation tests passed");

        LOGGER.info("Running bulk write operation tests");
        store1.writeAll(longEntries);
        store2.writeAll(personEntries);
        store3.writeAll(personEntries);
        LOGGER.info("Bulk write operation tests passed");

        LOGGER.info("BLOB strategy write tests passed");

        LOGGER.info("Running BLOB strategy read tests");

        LOGGER.info("Running single read operation tests");

        Long longVal = (Long) store1.load(longEntries.iterator().next().getKey());
        if (!longEntries.iterator().next().getValue().equals(longVal))
            throw new RuntimeException("Long values were incorrectly deserialized from Cassandra");

        Person personVal = (Person) store2.load(personEntries.iterator().next().getKey());
        if (!personEntries.iterator().next().getValue().equals(personVal))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        personVal = (Person) store3.load(personEntries.iterator().next().getKey());
        if (!personEntries.iterator().next().getValue().equals(personVal))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        LOGGER.info("Single read operation tests passed");

        LOGGER.info("Running bulk read operation tests");

        Map longValues = store1.loadAll(TestsHelper.getKeys(longEntries));
        if (!TestsHelper.checkCollectionsEqual(longValues, longEntries))
            throw new RuntimeException("Long values were incorrectly deserialized from Cassandra");

        Map personValues = store2.loadAll(TestsHelper.getKeys(personEntries));
        if (!TestsHelper.checkPersonCollectionsEqual(personValues, personEntries, false))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        personValues = store3.loadAll(TestsHelper.getKeys(personEntries));
        if (!TestsHelper.checkPersonCollectionsEqual(personValues, personEntries, false))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk read operation tests passed");

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

        CacheStore store4 = CacheStoreHelper.createCacheStore("persons",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-4.xml"),
                CassandraHelper.getAdminDataSrc());

        CacheStore store5 = CacheStoreHelper.createCacheStore("persons-handler",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-7-handler.xml"),
                CassandraHelper.getAdminDataSrc());

        CacheStore productStore = CacheStoreHelper.createCacheStore("product",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/product.xml"),
                CassandraHelper.getAdminDataSrc());

        CacheStore orderStore = CacheStoreHelper.createCacheStore("order",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/order.xml"),
                CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<Long, Person>> entries1 = TestsHelper.generateLongsPersonsEntries();
        Collection<CacheEntryImpl<PersonId, Person>> entries2 = TestsHelper.generatePersonIdsPersonsEntries();
        Collection<CacheEntryImpl<PersonId, Person>> entries3 = TestsHelper.generatePersonIdsPersonsEntries();
        Collection<CacheEntryImpl<PersonId, Person>> entries5 = TestsHelper.generatePersonIdsPersonsForHandlerEntries();
        Collection<CacheEntryImpl<Long, Product>> productEntries = TestsHelper.generateProductEntries();
        Collection<CacheEntryImpl<Long, ProductOrder>> orderEntries = TestsHelper.generateOrderEntries();

        LOGGER.info("Running POJO strategy write tests");

        LOGGER.info("Running single write operation tests");
        store1.write(entries1.iterator().next());
        store2.write(entries2.iterator().next());
        store3.write(entries3.iterator().next());
        store4.write(entries3.iterator().next());
        store5.write(entries5.iterator().next());
        productStore.write(productEntries.iterator().next());
        orderStore.write(orderEntries.iterator().next());
        LOGGER.info("Single write operation tests passed");

        LOGGER.info("Running bulk write operation tests");
        store1.writeAll(entries1);
        store2.writeAll(entries2);
        store3.writeAll(entries3);
        store4.writeAll(entries3);
        store5.writeAll(entries5);
        productStore.writeAll(productEntries);
        orderStore.writeAll(orderEntries);
        LOGGER.info("Bulk write operation tests passed");

        LOGGER.info("POJO strategy write tests passed");

        LOGGER.info("Running POJO strategy read tests");

        LOGGER.info("Running single read operation tests");

        Person person = (Person) store1.load(entries1.iterator().next().getKey());
        if (!entries1.iterator().next().getValue().equalsPrimitiveFields(person))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        person = (Person) store2.load(entries2.iterator().next().getKey());
        if (!entries2.iterator().next().getValue().equalsPrimitiveFields(person))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        person = (Person) store3.load(entries3.iterator().next().getKey());
        if (!entries3.iterator().next().getValue().equals(person))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        person = (Person) store4.load(entries3.iterator().next().getKey());
        if (!entries3.iterator().next().getValue().equals(person))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        person = (Person) store5.load(entries5.iterator().next().getKey());
        if (!entries5.iterator().next().getValue().equals(person))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        Product product = (Product) productStore.load(productEntries.iterator().next().getKey());
        if (!productEntries.iterator().next().getValue().equals(product))
            throw new RuntimeException("Product values were incorrectly deserialized from Cassandra");

        ProductOrder order = (ProductOrder) orderStore.load(orderEntries.iterator().next().getKey());
        if (!orderEntries.iterator().next().getValue().equals(order))
            throw new RuntimeException("Order values were incorrectly deserialized from Cassandra");

        LOGGER.info("Single read operation tests passed");

        LOGGER.info("Running bulk read operation tests");

        Map persons = store1.loadAll(TestsHelper.getKeys(entries1));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries1, true))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        persons = store2.loadAll(TestsHelper.getKeys(entries2));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries2, true))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        persons = store3.loadAll(TestsHelper.getKeys(entries3));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries3, false))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        persons = store4.loadAll(TestsHelper.getKeys(entries3));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries3, false))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        persons = store5.loadAll(TestsHelper.getKeys(entries5));
        if (!TestsHelper.checkPersonCollectionsEqual(persons, entries5, false))
            throw new RuntimeException("Person values were incorrectly deserialized from Cassandra");

        Map products = productStore.loadAll(TestsHelper.getKeys(productEntries));
        if (!TestsHelper.checkProductCollectionsEqual(products, productEntries))
            throw new RuntimeException("Product values were incorrectly deserialized from Cassandra");

        Map orders = orderStore.loadAll(TestsHelper.getKeys(orderEntries));
        if (!TestsHelper.checkOrderCollectionsEqual(orders, orderEntries))
            throw new RuntimeException("Order values were incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk read operation tests passed");

        LOGGER.info("POJO strategy read tests passed");

        LOGGER.info("Running POJO strategy delete tests");

        store1.delete(entries1.iterator().next().getKey());
        store1.deleteAll(TestsHelper.getKeys(entries1));

        store2.delete(entries2.iterator().next().getKey());
        store2.deleteAll(TestsHelper.getKeys(entries2));

        store3.delete(entries3.iterator().next().getKey());
        store3.deleteAll(TestsHelper.getKeys(entries3));

        store4.delete(entries3.iterator().next().getKey());
        store4.deleteAll(TestsHelper.getKeys(entries3));

        store5.delete(entries5.iterator().next().getKey());
        store5.deleteAll(TestsHelper.getKeys(entries5));

        productStore.delete(productEntries.iterator().next().getKey());
        productStore.deleteAll(TestsHelper.getKeys(productEntries));

        orderStore.delete(orderEntries.iterator().next().getKey());
        orderStore.deleteAll(TestsHelper.getKeys(orderEntries));

        LOGGER.info("POJO strategy delete tests passed");
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void pojoStrategySimpleObjectsTest() {
        CacheStore store5 = CacheStoreHelper.createCacheStore("persons5",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-5.xml"),
                CassandraHelper.getAdminDataSrc());

        CacheStore store6 = CacheStoreHelper.createCacheStore("persons6",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-6.xml"),
                CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<SimplePersonId, SimplePerson>> entries5 = TestsHelper.generateSimplePersonIdsPersonsEntries();
        Collection<CacheEntryImpl<SimplePersonId, SimplePerson>> entries6 = TestsHelper.generateSimplePersonIdsPersonsEntries();

        LOGGER.info("Running POJO strategy write tests for simple objects");

        LOGGER.info("Running single write operation tests");
        store5.write(entries5.iterator().next());
        store6.write(entries6.iterator().next());
        LOGGER.info("Single write operation tests passed");

        LOGGER.info("Running bulk write operation tests");
        store5.writeAll(entries5);
        store6.writeAll(entries6);
        LOGGER.info("Bulk write operation tests passed");

        LOGGER.info("POJO strategy write tests for simple objects passed");

        LOGGER.info("Running POJO simple objects strategy read tests");

        LOGGER.info("Running single read operation tests");

        SimplePerson person = (SimplePerson) store5.load(entries5.iterator().next().getKey());
        if (!entries5.iterator().next().getValue().equalsPrimitiveFields(person))
            throw new RuntimeException("SimplePerson values were incorrectly deserialized from Cassandra");

        person = (SimplePerson) store6.load(entries6.iterator().next().getKey());
        if (!entries6.iterator().next().getValue().equalsPrimitiveFields(person))
            throw new RuntimeException("SimplePerson values were incorrectly deserialized from Cassandra");

        LOGGER.info("Single read operation tests passed");

        LOGGER.info("Running bulk read operation tests");

        Map persons = store5.loadAll(TestsHelper.getKeys(entries5));
        if (!TestsHelper.checkSimplePersonCollectionsEqual(persons, entries5, true))
            throw new RuntimeException("SimplePerson values were incorrectly deserialized from Cassandra");

        persons = store6.loadAll(TestsHelper.getKeys(entries6));
        if (!TestsHelper.checkSimplePersonCollectionsEqual(persons, entries6, true))
            throw new RuntimeException("SimplePerson values were incorrectly deserialized from Cassandra");

        LOGGER.info("Bulk read operation tests passed");

        LOGGER.info("POJO strategy read tests for simple objects passed");

        LOGGER.info("Running POJO strategy delete tests for simple objects");

        store5.delete(entries5.iterator().next().getKey());
        store5.deleteAll(TestsHelper.getKeys(entries5));

        store6.delete(entries6.iterator().next().getKey());
        store6.deleteAll(TestsHelper.getKeys(entries6));

        LOGGER.info("POJO strategy delete tests for simple objects passed");
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void pojoStrategyTransactionTest() {
        Map<Object, Object> sessionProps = U.newHashMap(1);
        Transaction sessionTx = new TestTransaction();

        CacheStore productStore = CacheStoreHelper.createCacheStore("product",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/product.xml"),
                CassandraHelper.getAdminDataSrc(), new TestCacheSession("product", sessionTx, sessionProps));

        CacheStore orderStore = CacheStoreHelper.createCacheStore("order",
                new ClassPathResource("org/apache/ignite/tests/persistence/pojo/order.xml"),
                CassandraHelper.getAdminDataSrc(), new TestCacheSession("order", sessionTx, sessionProps));

        List<CacheEntryImpl<Long, Product>> productEntries = TestsHelper.generateProductEntries();
        Map<Long, List<CacheEntryImpl<Long, ProductOrder>>> ordersPerProduct =
                TestsHelper.generateOrdersPerProductEntries(productEntries, 2);

        Collection<Long> productIds = TestsHelper.getProductIds(productEntries);
        Collection<Long> orderIds = TestsHelper.getOrderIds(ordersPerProduct);

        LOGGER.info("Running POJO strategy transaction write tests");

        LOGGER.info("Running single write operation tests");

        CassandraHelper.dropTestKeyspaces();

        Product product = productEntries.iterator().next().getValue();
        ProductOrder order = ordersPerProduct.get(product.getId()).iterator().next().getValue();

        productStore.write(productEntries.iterator().next());
        orderStore.write(ordersPerProduct.get(product.getId()).iterator().next());

        if (productStore.load(product.getId()) != null || orderStore.load(order.getId()) != null) {
            throw new RuntimeException("Single write operation test failed. Transaction wasn't committed yet, but " +
                    "objects were already persisted into Cassandra");
        }

        Map<Long, Product> products = (Map<Long, Product>) productStore.loadAll(productIds);
        Map<Long, ProductOrder> orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if ((products != null && !products.isEmpty()) || (orders != null && !orders.isEmpty())) {
            throw new RuntimeException("Single write operation test failed. Transaction wasn't committed yet, but " +
                    "objects were already persisted into Cassandra");
        }

        //noinspection deprecation
        orderStore.sessionEnd(true);
        //noinspection deprecation
        productStore.sessionEnd(true);

        Product product1 = (Product) productStore.load(product.getId());
        ProductOrder order1 = (ProductOrder) orderStore.load(order.getId());

        if (product1 == null || order1 == null) {
            throw new RuntimeException("Single write operation test failed. Transaction was committed, but " +
                    "no objects were persisted into Cassandra");
        }

        if (!product.equals(product1) || !order.equals(order1)) {
            throw new RuntimeException("Single write operation test failed. Transaction was committed, but " +
                    "objects were incorrectly persisted/loaded to/from Cassandra");
        }

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if (products == null || products.isEmpty() || orders == null || orders.isEmpty()) {
            throw new RuntimeException("Single write operation test failed. Transaction was committed, but " +
                    "no objects were persisted into Cassandra");
        }

        if (products.size() > 1 || orders.size() > 1) {
            throw new RuntimeException("Single write operation test failed. There were committed more objects " +
                    "into Cassandra than expected");
        }

        product1 = products.entrySet().iterator().next().getValue();
        order1 = orders.entrySet().iterator().next().getValue();

        if (!product.equals(product1) || !order.equals(order1)) {
            throw new RuntimeException("Single write operation test failed. Transaction was committed, but " +
                    "objects were incorrectly persisted/loaded to/from Cassandra");
        }

        LOGGER.info("Single write operation tests passed");

        LOGGER.info("Running bulk write operation tests");

        CassandraHelper.dropTestKeyspaces();
        sessionProps.clear();

        productStore.writeAll(productEntries);

        for (Long productId : ordersPerProduct.keySet())
            orderStore.writeAll(ordersPerProduct.get(productId));

        for (Long productId : productIds) {
            if (productStore.load(productId) != null) {
                throw new RuntimeException("Bulk write operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already persisted into Cassandra");
            }
        }

        for (Long orderId : orderIds) {
            if (orderStore.load(orderId) != null) {
                throw new RuntimeException("Bulk write operation test failed. Transaction wasn't committed yet, but " +
                        "objects were already persisted into Cassandra");
            }
        }

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if ((products != null && !products.isEmpty()) || (orders != null && !orders.isEmpty())) {
            throw new RuntimeException("Bulk write operation test failed. Transaction wasn't committed yet, but " +
                    "objects were already persisted into Cassandra");
        }

        //noinspection deprecation
        productStore.sessionEnd(true);
        //noinspection deprecation
        orderStore.sessionEnd(true);

        for (CacheEntryImpl<Long, Product> entry : productEntries) {
            product = (Product) productStore.load(entry.getKey());

            if (!entry.getValue().equals(product)) {
                throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                        "not all objects were persisted into Cassandra");
            }
        }

        for (Long productId : ordersPerProduct.keySet()) {
            for (CacheEntryImpl<Long, ProductOrder> entry : ordersPerProduct.get(productId)) {
                order = (ProductOrder) orderStore.load(entry.getKey());

                if (!entry.getValue().equals(order)) {
                    throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                            "not all objects were persisted into Cassandra");
                }
            }
        }

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if (products == null || products.isEmpty() || orders == null || orders.isEmpty()) {
            throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                    "no objects were persisted into Cassandra");
        }

        if (products.size() < productIds.size() || orders.size() < orderIds.size()) {
            throw new RuntimeException("Bulk write operation test failed. There were committed less objects " +
                    "into Cassandra than expected");
        }

        if (products.size() > productIds.size() || orders.size() > orderIds.size()) {
            throw new RuntimeException("Bulk write operation test failed. There were committed more objects " +
                    "into Cassandra than expected");
        }

        for (CacheEntryImpl<Long, Product> entry : productEntries) {
            product = products.get(entry.getKey());

            if (!entry.getValue().equals(product)) {
                throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                        "some objects were incorrectly persisted/loaded to/from Cassandra");
            }
        }

        for (Long productId : ordersPerProduct.keySet()) {
            for (CacheEntryImpl<Long, ProductOrder> entry : ordersPerProduct.get(productId)) {
                order = orders.get(entry.getKey());

                if (!entry.getValue().equals(order)) {
                    throw new RuntimeException("Bulk write operation test failed. Transaction was committed, but " +
                            "some objects were incorrectly persisted/loaded to/from Cassandra");
                }
            }
        }

        LOGGER.info("Bulk write operation tests passed");

        LOGGER.info("POJO strategy transaction write tests passed");

        LOGGER.info("Running POJO strategy transaction delete tests");

        LOGGER.info("Running single delete tests");

        sessionProps.clear();

        Product deletedProduct = productEntries.remove(0).getValue();
        ProductOrder deletedOrder = ordersPerProduct.get(deletedProduct.getId()).remove(0).getValue();

        productStore.delete(deletedProduct.getId());
        orderStore.delete(deletedOrder.getId());

        if (productStore.load(deletedProduct.getId()) == null || orderStore.load(deletedOrder.getId()) == null) {
            throw new RuntimeException("Single delete operation test failed. Transaction wasn't committed yet, but " +
                    "objects were already deleted from Cassandra");
        }

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if (products.size() != productIds.size() || orders.size() != orderIds.size()) {
            throw new RuntimeException("Single delete operation test failed. Transaction wasn't committed yet, but " +
                    "objects were already deleted from Cassandra");
        }

        //noinspection deprecation
        productStore.sessionEnd(true);
        //noinspection deprecation
        orderStore.sessionEnd(true);

        if (productStore.load(deletedProduct.getId()) != null || orderStore.load(deletedOrder.getId()) != null) {
            throw new RuntimeException("Single delete operation test failed. Transaction was committed, but " +
                    "objects were not deleted from Cassandra");
        }

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if (products.get(deletedProduct.getId()) != null || orders.get(deletedOrder.getId()) != null) {
            throw new RuntimeException("Single delete operation test failed. Transaction was committed, but " +
                    "objects were not deleted from Cassandra");
        }

        LOGGER.info("Single delete tests passed");

        LOGGER.info("Running bulk delete tests");

        sessionProps.clear();

        productStore.deleteAll(productIds);
        orderStore.deleteAll(orderIds);

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if (products == null || products.isEmpty() || orders == null || orders.isEmpty()) {
            throw new RuntimeException("Bulk delete operation test failed. Transaction wasn't committed yet, but " +
                    "objects were already deleted from Cassandra");
        }

        //noinspection deprecation
        orderStore.sessionEnd(true);
        //noinspection deprecation
        productStore.sessionEnd(true);

        products = (Map<Long, Product>) productStore.loadAll(productIds);
        orders = (Map<Long, ProductOrder>) orderStore.loadAll(orderIds);

        if ((products != null && !products.isEmpty()) || (orders != null && !orders.isEmpty())) {
            throw new RuntimeException("Bulk delete operation test failed. Transaction was committed, but " +
                    "objects were not deleted from Cassandra");
        }

        LOGGER.info("Bulk delete tests passed");

        LOGGER.info("POJO strategy transaction delete tests passed");
    }
}
