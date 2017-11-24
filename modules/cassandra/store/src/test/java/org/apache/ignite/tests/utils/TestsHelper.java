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


import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.load.Generator;
import org.springframework.core.io.ClassPathResource;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.Calendar;
import java.util.Date;

import org.apache.ignite.tests.pojos.Product;
import org.apache.ignite.tests.pojos.ProductOrder;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.SimplePerson;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.pojos.SimplePersonId;

/**
 * Helper class for all tests
 */
public class TestsHelper {
    /** */
    private static final String LETTERS_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /** */
    private static final String NUMBERS_ALPHABET = "0123456789";

    /** */
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    /** */
    private static final ResourceBundle TESTS_SETTINGS = ResourceBundle.getBundle("tests");

    /** */
    private static final int BULK_OPERATION_SIZE = parseTestSettings("bulk.operation.size");

    /** */
    private static final String LOAD_TESTS_CACHE_NAME = TESTS_SETTINGS.getString("load.tests.cache.name");

    /** */
    private static final int LOAD_TESTS_THREADS_COUNT = parseTestSettings("load.tests.threads.count");

    /** */
    private static final int LOAD_TESTS_WARMUP_PERIOD = parseTestSettings("load.tests.warmup.period");

    /** */
    private static final int LOAD_TESTS_EXECUTION_TIME = parseTestSettings("load.tests.execution.time");

    /** */
    private static final int LOAD_TESTS_REQUESTS_LATENCY = parseTestSettings("load.tests.requests.latency");

    /** */
    private static final int TRANSACTION_PRODUCTS_COUNT = parseTestSettings("transaction.products.count");

    /** */
    private static final int TRANSACTION_ORDERS_COUNT = parseTestSettings("transaction.orders.count");

    /** */
    private static final int ORDERS_YEAR;

    /** */
    private static final int ORDERS_MONTH;

    /** */
    private static final int ORDERS_DAY;

    /** */
    private static final String LOAD_TESTS_PERSISTENCE_SETTINGS = TESTS_SETTINGS.getString("load.tests.persistence.settings");

    /** */
    private static final String LOAD_TESTS_IGNITE_CONFIG = TESTS_SETTINGS.getString("load.tests.ignite.config");

    /** */
    private static final Generator LOAD_TESTS_KEY_GENERATOR;

    /** */
    private static final Generator LOAD_TESTS_VALUE_GENERATOR;

    /** */
    private static final String HOST_PREFIX;

    static {
        try {
            LOAD_TESTS_KEY_GENERATOR = (Generator)Class.forName(TESTS_SETTINGS.getString("load.tests.key.generator")).newInstance();
            LOAD_TESTS_VALUE_GENERATOR = (Generator)Class.forName(TESTS_SETTINGS.getString("load.tests.value.generator")).newInstance();

            String[] parts = SystemHelper.HOST_IP.split("\\.");

            String prefix = parts[3];
            prefix = prefix.length() > 2 ? prefix.substring(prefix.length() - 2) : prefix;

            HOST_PREFIX = prefix;

            Calendar cl = Calendar.getInstance();

            String year = TESTS_SETTINGS.getString("orders.year");
            ORDERS_YEAR = !year.trim().isEmpty() ? Integer.parseInt(year) : cl.get(Calendar.YEAR);

            String month = TESTS_SETTINGS.getString("orders.month");
            ORDERS_MONTH = !month.trim().isEmpty() ? Integer.parseInt(month) : cl.get(Calendar.MONTH);

            String day = TESTS_SETTINGS.getString("orders.day");
            ORDERS_DAY = !day.trim().isEmpty() ? Integer.parseInt(day) : cl.get(Calendar.DAY_OF_MONTH);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to initialize TestsHelper", e);
        }
    }

    /** */
    private static int parseTestSettings(String name) {
        return Integer.parseInt(TESTS_SETTINGS.getString(name));
    }

    /** */
    public static int getLoadTestsThreadsCount() {
        return LOAD_TESTS_THREADS_COUNT;
    }

    /** */
    public static int getLoadTestsWarmupPeriod() {
        return LOAD_TESTS_WARMUP_PERIOD;
    }

    /** */
    public static int getLoadTestsExecutionTime() {
        return LOAD_TESTS_EXECUTION_TIME;
    }

    /** */
    public static int getLoadTestsRequestsLatency() {
        return LOAD_TESTS_REQUESTS_LATENCY;
    }

    /** */
    public static ClassPathResource getLoadTestsPersistenceSettings() {
        return new ClassPathResource(LOAD_TESTS_PERSISTENCE_SETTINGS);
    }

    /** */
    public static String getLoadTestsIgniteConfig() {
        return LOAD_TESTS_IGNITE_CONFIG;
    }

    /** */
    public static int getBulkOperationSize() {
        return BULK_OPERATION_SIZE;
    }

    /** */
    public static String getLoadTestsCacheName() {
        return LOAD_TESTS_CACHE_NAME;
    }

    /** */
    public static Object generateLoadTestsKey(long i) {
        return LOAD_TESTS_KEY_GENERATOR.generate(i);
    }

    /** */
    public static Object generateLoadTestsValue(long i) {
        return LOAD_TESTS_VALUE_GENERATOR.generate(i);
    }

    /** */
    @SuppressWarnings("unchecked")
    public static CacheEntryImpl generateLoadTestsEntry(long i) {
        return new CacheEntryImpl(TestsHelper.generateLoadTestsKey(i), TestsHelper.generateLoadTestsValue(i));
    }

    /** */
    public static <K, V> Collection<K> getKeys(Collection<CacheEntryImpl<K, V>> entries) {
        List<K> list = new LinkedList<>();

        for (CacheEntryImpl<K, ?> entry : entries)
            list.add(entry.getKey());

        return list;
    }

    /** */
    public static Map<Long, Long> generateLongsMap() {
        return generateLongsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<Long, Long> generateLongsMap(int cnt) {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < cnt; i++)
            map.put(i, i + 123);

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<Long, Long>> generateLongsEntries() {
        return generateLongsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<Long, Long>> generateLongsEntries(int cnt) {
        Collection<CacheEntryImpl<Long, Long>> entries = new LinkedList<>();

        for (long i = 0; i < cnt; i++)
            entries.add(new CacheEntryImpl<>(i, i + 123));

        return entries;
    }

    /** */
    public static Collection<CacheEntryImpl<Long, String>> generateLongStringEntries() {
        return generateLongStringEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<Long, String>> generateLongStringEntries(int cnt) {
        Collection<CacheEntryImpl<Long, String>> entries = new LinkedList<>();

        for (long i = 0; i < cnt; i++)
            entries.add(new CacheEntryImpl<>(i, Long.toString(i + 123)));

        return entries;
    }

    /** */
    public static Map<String, String> generateStringsMap() {
        return generateStringsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<String, String> generateStringsMap(int cnt) {
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(Integer.toString(i), randomString(5));

        return map;
    }

    /** */
    public static Map<Long, String> generateLongStringMap() {
        return generateLongStringMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<Long, String> generateLongStringMap(int cnt) {
        Map<Long, String> map = new HashMap<>();

        for (long i = 0; i < cnt; i++)
            map.put(i, Long.toString(i+123));

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<String, String>> generateStringsEntries() {
        return generateStringsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<String, String>> generateStringsEntries(int cnt) {
        Collection<CacheEntryImpl<String, String>> entries = new LinkedList<>();

        for (int i = 0; i < cnt; i++)
            entries.add(new CacheEntryImpl<>(Integer.toString(i), randomString(5)));

        return entries;
    }

    /** */
    public static Map<Long, Person> generateLongsPersonsMap() {
        Map<Long, Person> map = new HashMap<>();

        for (long i = 0; i < BULK_OPERATION_SIZE; i++)
            map.put(i, generateRandomPerson(i));

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<Long, Person>> generateLongsPersonsEntries() {
        Collection<CacheEntryImpl<Long, Person>> entries = new LinkedList<>();

        for (long i = 0; i < BULK_OPERATION_SIZE; i++)
            entries.add(new CacheEntryImpl<>(i, generateRandomPerson(i)));

        return entries;
    }

    /** */
    public static Map<SimplePersonId, SimplePerson> generateSimplePersonIdsPersonsMap() {
        return generateSimplePersonIdsPersonsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<SimplePersonId, SimplePerson> generateSimplePersonIdsPersonsMap(int cnt) {
        Map<SimplePersonId, SimplePerson> map = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            PersonId id = generateRandomPersonId();

            map.put(new SimplePersonId(id), new SimplePerson(generateRandomPerson(id.getPersonNumber())));
        }

        return map;
    }

    /** */
    public static Map<PersonId, Person> generatePersonIdsPersonsMap() {
        return generatePersonIdsPersonsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<PersonId, Person> generatePersonIdsPersonsMap(int cnt) {
        Map<PersonId, Person> map = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            PersonId id = generateRandomPersonId();

            map.put(id, generateRandomPerson(id.getPersonNumber()));
        }

        return map;
    }

    /** */
    public static Map<PersonId, Person> generatePersonIdsPersonsForHandlerMap() {
        return generatePersonIdsPersonsForHandlerMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<PersonId, Person> generatePersonIdsPersonsForHandlerMap(int cnt) {
        Map<PersonId, Person> map = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            PersonId id = generateRandomPersonIdForHandler();

            map.put(id, generateRandomPersonForHandler(id.getPersonNumber()));
        }

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<SimplePersonId, SimplePerson>> generateSimplePersonIdsPersonsEntries() {
        return generateSimplePersonIdsPersonsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<SimplePersonId, SimplePerson>> generateSimplePersonIdsPersonsEntries(int cnt) {
        Collection<CacheEntryImpl<SimplePersonId, SimplePerson>> entries = new LinkedList<>();

        for (int i = 0; i < cnt; i++) {
            PersonId id = generateRandomPersonId();

            entries.add(new CacheEntryImpl<>(new SimplePersonId(id), new SimplePerson(generateRandomPerson(id.getPersonNumber()))));
        }

        return entries;
    }

    /** */
    public static Collection<CacheEntryImpl<PersonId, Person>> generatePersonIdsPersonsEntries() {
        return generatePersonIdsPersonsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<PersonId, Person>> generatePersonIdsPersonsEntries(int cnt) {
        Collection<CacheEntryImpl<PersonId, Person>> entries = new LinkedList<>();

        for (int i = 0; i < cnt; i++) {
            PersonId id = generateRandomPersonId();

            entries.add(new CacheEntryImpl<>(id, generateRandomPerson(id.getPersonNumber())));
        }

        return entries;
    }

    /** */
    public static Collection<CacheEntryImpl<PersonId, Person>> generatePersonIdsPersonsForHandlerEntries() {
        return generatePersonIdsPersonsForHandlerEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<PersonId, Person>> generatePersonIdsPersonsForHandlerEntries(int cnt) {
        Collection<CacheEntryImpl<PersonId, Person>> entries = new LinkedList<>();

        for (int i = 0; i < cnt; i++) {
            PersonId id = generateRandomPersonIdForHandler();

            entries.add(new CacheEntryImpl<>(id, generateRandomPersonForHandler(id.getPersonNumber())));
        }

        return entries;
    }

    /** */
    public static List<CacheEntryImpl<Long, Product>> generateProductEntries() {
        List<CacheEntryImpl<Long, Product>> entries = new LinkedList<>();

        for (long i = 0; i < BULK_OPERATION_SIZE; i++)
            entries.add(new CacheEntryImpl<>(i, generateRandomProduct(i)));

        return entries;
    }

    /** */
    public static Collection<Long> getProductIds(Collection<CacheEntryImpl<Long, Product>> entries) {
        List<Long> ids = new LinkedList<>();

        for (CacheEntryImpl<Long, Product> entry : entries)
            ids.add(entry.getKey());

        return ids;
    }

    /** */
    public static Map<Long, Product> generateProductsMap() {
        return generateProductsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<Long, Product> generateProductsMap(int count) {
        Map<Long, Product> map = new HashMap<>();

        for (long i = 0; i < count; i++)
            map.put(i, generateRandomProduct(i));

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<Long, ProductOrder>> generateOrderEntries() {
        Collection<CacheEntryImpl<Long, ProductOrder>> entries = new LinkedList<>();

        for (long i = 0; i < BULK_OPERATION_SIZE; i++) {
            ProductOrder order = generateRandomOrder(i);
            entries.add(new CacheEntryImpl<>(order.getId(), order));
        }

        return entries;
    }

    /** */
    public static Map<Long, ProductOrder> generateOrdersMap() {
        return generateOrdersMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<Long, ProductOrder> generateOrdersMap(int count) {
        Map<Long, ProductOrder> map = new HashMap<>();

        for (long i = 0; i < count; i++) {
            ProductOrder order = generateRandomOrder(i);
            map.put(order.getId(), order);
        }

        return map;
    }

    /** */
    public static Map<Long, List<CacheEntryImpl<Long, ProductOrder>>> generateOrdersPerProductEntries(
            Collection<CacheEntryImpl<Long, Product>> products) {
        return generateOrdersPerProductEntries(products, TRANSACTION_ORDERS_COUNT);
    }

    /** */
    public static Map<Long, List<CacheEntryImpl<Long, ProductOrder>>> generateOrdersPerProductEntries(
            Collection<CacheEntryImpl<Long, Product>> products, int ordersPerProductCount) {
        Map<Long, List<CacheEntryImpl<Long, ProductOrder>>> map = new HashMap<>();

        for (CacheEntryImpl<Long, Product> entry : products) {
            List<CacheEntryImpl<Long, ProductOrder>> orders = new LinkedList<>();

            for (long i = 0; i < ordersPerProductCount; i++) {
                ProductOrder order = generateRandomOrder(entry.getKey());
                orders.add(new CacheEntryImpl<>(order.getId(), order));
            }

            map.put(entry.getKey(), orders);
        }

        return map;
    }

    /** */
    public static Map<Long, Map<Long, ProductOrder>> generateOrdersPerProductMap(Map<Long, Product> products) {
        return generateOrdersPerProductMap(products, TRANSACTION_ORDERS_COUNT);
    }

    /** */
    public static Map<Long, Map<Long, ProductOrder>> generateOrdersPerProductMap(Map<Long, Product> products,
                                                                                 int ordersPerProductCount) {
        Map<Long, Map<Long, ProductOrder>> map = new HashMap<>();

        for (Map.Entry<Long, Product> entry : products.entrySet()) {
            Map<Long, ProductOrder> orders = new HashMap<>();

            for (long i = 0; i < ordersPerProductCount; i++) {
                ProductOrder order = generateRandomOrder(entry.getKey());
                orders.put(order.getId(), order);
            }

            map.put(entry.getKey(), orders);
        }

        return map;
    }

    public static Collection<Long> getOrderIds(Map<Long, List<CacheEntryImpl<Long, ProductOrder>>> orders) {
        Set<Long> ids = new HashSet<>();

        for (Long key : orders.keySet()) {
            for (CacheEntryImpl<Long, ProductOrder> entry : orders.get(key))
                ids.add(entry.getKey());
        }

        return ids;
    }

    /** */
    public static SimplePerson generateRandomSimplePerson(long personNum) {
        int phonesCnt = RANDOM.nextInt(4);

        List<String> phones = new LinkedList<>();

        for (int i = 0; i < phonesCnt; i++)
            phones.add(randomNumber(4));

        return new SimplePerson(personNum, randomString(4), randomString(4), RANDOM.nextInt(100),
                RANDOM.nextBoolean(), RANDOM.nextLong(), RANDOM.nextFloat(), new Date(), phones);
    }

    /** */
    public static SimplePersonId generateRandomSimplePersonId() {
        return new SimplePersonId(randomString(4), randomString(4), RANDOM.nextInt(100));
    }

    /** */
    public static Person generateRandomPerson(long personNum) {
        int phonesCnt = RANDOM.nextInt(4);

        List<String> phones = new LinkedList<>();

        for (int i = 0; i < phonesCnt; i++)
            phones.add(randomNumber(4));

        return new Person(personNum, randomString(4), randomString(4), RANDOM.nextInt(100),
            RANDOM.nextBoolean(), RANDOM.nextLong(), RANDOM.nextFloat(), new Date(), phones);
    }
    /** */
    public static Person generateRandomPersonForHandler(long personNum) {
        int phonesCnt = RANDOM.nextInt(4);

        List<String> phones = new LinkedList<>();

        for (int i = 0; i < phonesCnt; i++)
            phones.add(randomNumber(4));

        return new Person(personNum, randomString(4), Long.toString(RANDOM.nextInt(100)), RANDOM.nextInt(100),
            RANDOM.nextBoolean(), RANDOM.nextLong(), RANDOM.nextFloat(), new Date(), phones);
    }

    /** */
    public static PersonId generateRandomPersonId() {
        return new PersonId(randomString(4), randomString(4), RANDOM.nextInt(100));
    }

    /** */
    public static PersonId generateRandomPersonIdForHandler() {
        return new PersonId(Long.toString(RANDOM.nextInt(100)), randomString(4), RANDOM.nextInt(100));
    }

    /** */
    public static Product generateRandomProduct(long id) {
        return new Product(id, randomString(2), randomString(6), randomString(20), generateProductPrice(id));
    }

    /** */
    public static ProductOrder generateRandomOrder(long productId) {
        return generateRandomOrder(productId, RANDOM.nextInt(10000));
    }

    /** */
    private static ProductOrder generateRandomOrder(long productId, int saltedNumber) {
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.YEAR, ORDERS_YEAR);
        cl.set(Calendar.MONTH, ORDERS_MONTH);
        cl.set(Calendar.DAY_OF_MONTH, ORDERS_DAY);

        long id = Long.parseLong(productId + System.currentTimeMillis() + HOST_PREFIX + saltedNumber);

        return generateRandomOrder(id, productId, cl.getTime());
    }

    /** */
    public static ProductOrder generateRandomOrder(long id, long productId, Date date) {
        return new ProductOrder(id, productId, generateProductPrice(productId), date, 1 + RANDOM.nextInt(20));
    }

    /** */
    public static boolean checkMapsEqual(Map map1, Map map2) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (Object key : map1.keySet()) {
            Object obj1 = map1.get(key);
            Object obj2 = map2.get(key);

            if (obj1 == null || obj2 == null || !obj1.equals(obj2))
                return false;
        }

        return true;
    }

    /** */
    public static <K, V> boolean checkCollectionsEqual(Map<K, V> map, Collection<CacheEntryImpl<K, V>> col) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, V> entry : col) {
            if (!entry.getValue().equals(map.get(entry.getKey())))
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkSimplePersonMapsEqual(Map<K, SimplePerson> map1, Map<K, SimplePerson> map2,
                                                   boolean primitiveFieldsOnly) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (K key : map1.keySet()) {
            SimplePerson person1 = map1.get(key);
            SimplePerson person2 = map2.get(key);

            boolean equals = person1 != null && person2 != null &&
                    (primitiveFieldsOnly ? person1.equalsPrimitiveFields(person2) : person1.equals(person2));

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkPersonMapsEqual(Map<K, Person> map1, Map<K, Person> map2,
        boolean primitiveFieldsOnly) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (K key : map1.keySet()) {
            Person person1 = map1.get(key);
            Person person2 = map2.get(key);

            boolean equals = person1 != null && person2 != null &&
                (primitiveFieldsOnly ? person1.equalsPrimitiveFields(person2) : person1.equals(person2));

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkSimplePersonCollectionsEqual(Map<K, SimplePerson> map, Collection<CacheEntryImpl<K, SimplePerson>> col,
                                                          boolean primitiveFieldsOnly) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, SimplePerson> entry : col) {
            boolean equals = primitiveFieldsOnly ?
                    entry.getValue().equalsPrimitiveFields(map.get(entry.getKey())) :
                    entry.getValue().equals(map.get(entry.getKey()));

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkPersonCollectionsEqual(Map<K, Person> map, Collection<CacheEntryImpl<K, Person>> col,
        boolean primitiveFieldsOnly) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, Person> entry : col) {
            boolean equals = primitiveFieldsOnly ?
                entry.getValue().equalsPrimitiveFields(map.get(entry.getKey())) :
                entry.getValue().equals(map.get(entry.getKey()));

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkProductCollectionsEqual(Map<K, Product> map, Collection<CacheEntryImpl<K, Product>> col) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, Product> entry : col)
            if (!entry.getValue().equals(map.get(entry.getKey())))
                return false;

        return true;
    }

    /** */
    public static <K> boolean checkProductMapsEqual(Map<K, Product> map1, Map<K, Product> map2) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (K key : map1.keySet()) {
            Product product1 = map1.get(key);
            Product product2 = map2.get(key);

            boolean equals = product1 != null && product2 != null && product1.equals(product2);

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkOrderCollectionsEqual(Map<K, ProductOrder> map, Collection<CacheEntryImpl<K, ProductOrder>> col) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, ProductOrder> entry : col)
            if (!entry.getValue().equals(map.get(entry.getKey())))
                return false;

        return true;
    }

    /** */
    public static <K> boolean checkOrderMapsEqual(Map<K, ProductOrder> map1, Map<K, ProductOrder> map2) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (K key : map1.keySet()) {
            ProductOrder order1 = map1.get(key);
            ProductOrder order2 = map2.get(key);

            boolean equals = order1 != null && order2 != null && order1.equals(order2);

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static String randomString(int len) {
        StringBuilder builder = new StringBuilder(len);

        for (int i = 0; i < len; i++)
            builder.append(LETTERS_ALPHABET.charAt(RANDOM.nextInt(LETTERS_ALPHABET.length())));

        return builder.toString();
    }

    /** */
    public static String randomNumber(int len) {
        StringBuilder builder = new StringBuilder(len);

        for (int i = 0; i < len; i++)
            builder.append(NUMBERS_ALPHABET.charAt(RANDOM.nextInt(NUMBERS_ALPHABET.length())));

        return builder.toString();
    }

    /** */
    private static float generateProductPrice(long productId) {
        long id = productId < 1000 ?
                (((productId + 1) * (productId + 1) * 1000) / 2) * 10 :
                (productId / 20) * (productId / 20);

        id = id == 0 ? 24 : id;

        float price = Long.parseLong(Long.toString(id).replace("0", ""));

        int i = 0;

        while (price > 100) {
            if (i % 2 != 0)
                price = price / 2;
            else
                price = (float) Math.sqrt(price);

            i++;
        }

        return ((float)((int)(price * 100))) / 100.0F;
    }
}
