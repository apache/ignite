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

package org.apache.ignite.internal.processors.sql;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IgniteCachePartitionedAtomicColumnConstraintsTest extends AbstractIndexingCommonTest {
    /** */
    private static final long FUT_TIMEOUT = 10_000L;

    /** */
    private static final String STR_CACHE_NAME = "STR_STR";

    /** */
    private static final String STR_ORG_CACHE_NAME = "STR_ORG";

    /** */
    private static final String STR_ORG_WITH_FIELDS_CACHE_NAME = "STR_ORG_WITH_FIELDS";

    /** */
    private static final String OBJ_CACHE_NAME = "ORG_ADDRESS";

    /** */
    private static final String DEC_CACHE_NAME_FOR_SCALE = "DEC_DEC_FOR_SCALE";

    /** */
    private static final String OBJ_CACHE_NAME_FOR_SCALE = "ORG_EMPLOYEE_FOR_SCALE";

    /** */
    private static final String DEC_EMPL_CACHE_NAME_FOR_SCALE = "DEC_EMPLOYEE_FOR_SCALE";

    /** */
    private static final String DEC_CACHE_NAME_FOR_PREC = "DEC_DEC_FOR_PREC";

    /** */
    private static final String OBJ_CACHE_NAME_FOR_PREC = "ORG_EMPLOYEE_FOR_PREC";

    /** */
    private static final String DEC_EMPL_CACHE_NAME_FOR_PREC = "DEC_EMPLOYEE_FOR_PREC";

    /** */
    private Consumer<Runnable> shouldFail = (op) -> assertThrowsWithCause(op, IgniteException.class);

    /** */
    private Consumer<Runnable> shouldSucceed = Runnable::run;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        createCachesForStringTests();

        createCachesForDecimalPrecisionTests();

        createCachesForDecimalScaleTests();
    }

    /** @throws Exception If failed.*/
    private void createCachesForStringTests() throws Exception {
        Map<String, Integer> strStrPrecision = new HashMap<>();

        strStrPrecision.put(KEY_FIELD_NAME, 5);
        strStrPrecision.put(VAL_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), String.class.getName())
            .setFieldsPrecision(strStrPrecision)), STR_CACHE_NAME);

        Map<String, Integer> orgAddressPrecision = new HashMap<>();

        orgAddressPrecision.put("name", 5);
        orgAddressPrecision.put("address", 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(Organization.class.getName(), Address.class.getName())
            .setKeyFields(Collections.singleton("name"))
            .addQueryField("name", "java.lang.String", "name")
            .addQueryField("address", "java.lang.String", "address")
            .setFieldsPrecision(orgAddressPrecision)), OBJ_CACHE_NAME);

        Map<String, Integer> strOrgPrecision = new HashMap<>();

        strOrgPrecision.put(KEY_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), Organization.class.getName())
            .setFieldsPrecision(strOrgPrecision)), STR_ORG_CACHE_NAME);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), Organization.class.getName())
            .addQueryField("name", "java.lang.String", "name")
            .addQueryField("address", "java.lang.String", "address")
            .setFieldsPrecision(strOrgPrecision)), STR_ORG_WITH_FIELDS_CACHE_NAME);
    }

    /** @throws Exception If failed.*/
    private void createCachesForDecimalPrecisionTests() throws Exception {
        Map<String, Integer> decDecPrecision = new HashMap<>();

        decDecPrecision.put(KEY_FIELD_NAME, 4);
        decDecPrecision.put(VAL_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), BigDecimal.class.getName())
            .setFieldsPrecision(decDecPrecision)), DEC_CACHE_NAME_FOR_PREC);

        Map<String, Integer> orgEmployeePrecision = new HashMap<>();

        orgEmployeePrecision.put("id", 4);
        orgEmployeePrecision.put("salary", 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(DecOrganization.class.getName(), Employee.class.getName())
            .setKeyFields(Collections.singleton("id"))
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(orgEmployeePrecision)), OBJ_CACHE_NAME_FOR_PREC);

        Map<String, Integer> decEmployeePrecision = new HashMap<>();

        decEmployeePrecision.put(KEY_FIELD_NAME, 4);
        decEmployeePrecision.put("salary", 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), Employee.class.getName())
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(decEmployeePrecision)), DEC_EMPL_CACHE_NAME_FOR_PREC);
    }

    /** @throws Exception If failed.*/
    private void createCachesForDecimalScaleTests() throws Exception {
        Map<String, Integer> decDecPrecision = new HashMap<>();

        decDecPrecision.put(KEY_FIELD_NAME, 4);
        decDecPrecision.put(VAL_FIELD_NAME, 4);

        Map<String, Integer> decDecScale = new HashMap<>();

        decDecScale.put(KEY_FIELD_NAME, 2);
        decDecScale.put(VAL_FIELD_NAME, 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), BigDecimal.class.getName())
            .setFieldsScale(decDecScale)
            .setFieldsPrecision(decDecPrecision)), DEC_CACHE_NAME_FOR_SCALE);

        Map<String, Integer> orgEmployeePrecision = new HashMap<>();

        orgEmployeePrecision.put("id", 4);
        orgEmployeePrecision.put("salary", 4);

        Map<String, Integer> orgEmployeeScale = new HashMap<>();

        orgEmployeeScale.put("id", 2);
        orgEmployeeScale.put("salary", 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(DecOrganization.class.getName(), Employee.class.getName())
            .setKeyFields(Collections.singleton("id"))
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsScale(orgEmployeeScale)
            .setFieldsPrecision(orgEmployeePrecision)), OBJ_CACHE_NAME_FOR_SCALE);

        Map<String, Integer> decEmployeePrecision = new HashMap<>();

        decEmployeePrecision.put(KEY_FIELD_NAME, 4);
        decEmployeePrecision.put("salary", 4);

        Map<String, Integer> decEmployeeScale = new HashMap<>();

        decEmployeeScale.put(KEY_FIELD_NAME, 2);
        decEmployeeScale.put("salary", 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), Employee.class.getName())
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(decEmployeePrecision)
            .setFieldsScale(decEmployeeScale)), DEC_EMPL_CACHE_NAME_FOR_SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongStringValueFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("3", "123456");

        checkPutAll(shouldFail, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, "1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongStringKeyFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("123456", "2");

        checkPutAll(shouldFail, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongStringValueFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("3"), new Address("123456"));

        checkPutAll(shouldFail, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Address("1"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongStringKeyFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("123456"), new Address("2"));

        checkPutAll(shouldFail, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongStringKeyFail2() throws Exception {
        doCheckPutTooLongStringKeyFail2(STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongStringKeyFail3() throws Exception {
        doCheckPutTooLongStringKeyFail2(STR_ORG_WITH_FIELDS_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    private void doCheckPutTooLongStringKeyFail2(String cacheName) {
        IgniteCache<String, Organization> cache = jcache(0, cacheName);

        T2<String, Organization> val = new T2<>("123456", new Organization("1"));

        checkPutAll(shouldFail, cache, new T2<>("1", new Organization("1")), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutLongStringValue() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("3", "12345");

        checkPutAll(shouldSucceed, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, "1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutLongStringKey() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("12345", "2");

        checkPutAll(shouldSucceed, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutLongStringValueField() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("3"), new Address("12345"));

        checkPutAll(shouldSucceed, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Address("1"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutLongStringKeyField() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("12345"), new Address("2"));

        checkPutAll(shouldSucceed, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutLongStringKey2() throws Exception {
        doCheckPutLongStringKey2(STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutLongStringKey3() throws Exception {
        doCheckPutLongStringKey2(STR_ORG_WITH_FIELDS_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    private void doCheckPutLongStringKey2(String cacheName) {
        IgniteCache<String, Organization> cache = jcache(0, cacheName);

        T2<String, Organization> key2 = new T2<>("12345", new Organization("1"));

        checkPutAll(shouldSucceed, cache, new T2<>("1", new Organization("1")), key2);

        checkPutOps(shouldSucceed, cache, key2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalValueFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, BigDecimal> val = new T2<>(d(12.36), d(123.45));

        checkPutAll(shouldFail, cache, new T2<>(d(12.34), d(12.34)), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, d(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalKeyFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, BigDecimal> val = new T2<>(d(123.45), d(12.34));

        checkPutAll(shouldFail, cache, new T2<>(d(12.35), d(12.34)), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalKeyFail2() throws Exception {
        IgniteCache<BigDecimal, Employee> cache = jcache(0, DEC_EMPL_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, Employee> val = new T2<>(d(123.45), new Employee(d(12.34)));

        checkPutAll(shouldFail, cache, new T2<>(d(12.35), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalValueFieldFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_PREC);

        T2<DecOrganization, Employee> val = new T2<>(new DecOrganization(d(12.36)), new Employee(d(123.45)));

        checkPutAll(shouldFail, cache, new T2<>(new DecOrganization(d(12.34)), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(d(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalValueFieldFail2() throws Exception {
        IgniteCache<BigDecimal, Employee> cache = jcache(0, DEC_EMPL_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, Employee> val = new T2<>(d(12.36), new Employee(d(123.45)));

        checkPutAll(shouldFail, cache, new T2<>(d(12.34), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(d(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalKeyFieldFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_PREC);

        T2<DecOrganization, Employee> val = new T2<>(new DecOrganization(d(123.45)), new Employee(d(12.34)));

        checkPutAll(shouldFail, cache, new T2<>(new DecOrganization(d(12.35)), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalValueScaleFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(d(12.36), d(3.456));

        checkPutAll(shouldFail, cache, new T2<>(d(12.34), d(12.34)), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, d(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalKeyScaleFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(d(3.456), d(12.34));

        checkPutAll(shouldFail, cache, new T2<>(d(12.35), d(12.34)), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalKeyScaleFail2() throws Exception {
        IgniteCache<BigDecimal, Employee> cache = jcache(0, DEC_EMPL_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, Employee> val = new T2<>(d(3.456), new Employee(d(12.34)));

        checkPutAll(shouldFail, cache, new T2<>(d(12.35), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalValueFieldScaleFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_SCALE);

        T2<DecOrganization, Employee> val = new T2<>(new DecOrganization(d(12.36)), new Employee(d(3.456)));

        checkPutAll(shouldFail, cache, new T2<>(new DecOrganization(d(12.34)), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(d(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalValueFieldScaleFail2() throws Exception {
        IgniteCache<BigDecimal, Employee> cache = jcache(0, DEC_EMPL_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, Employee> val = new T2<>(d(12.36), new Employee(d(3.456)));

        checkPutAll(shouldFail, cache, new T2<>(d(12.34), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(d(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTooLongDecimalKeyFieldScaleFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_SCALE);

        T2<DecOrganization, Employee> val = new T2<>(new DecOrganization(d(3.456)), new Employee(d(12.34)));

        checkPutAll(shouldFail, cache, new T2<>(new DecOrganization(d(12.35)), new Employee(d(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutValidDecimalKeyAndValue() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(d(12.37), d(12.34));

        checkPutAll(shouldSucceed, cache, new T2<>(d(12.36), d(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, d(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutValidDecimalKeyAndValueField() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_SCALE);

        T2<DecOrganization, Employee> val = new T2<>(new DecOrganization(d(12.37)), new Employee(d(12.34)));

        checkPutAll(shouldSucceed, cache, new T2<>(new DecOrganization(d(12.36)), new Employee(d(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Employee(d(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutValidDecimalKeyAndValueField2() throws Exception {
        IgniteCache<BigDecimal, Employee> cache = jcache(0, DEC_EMPL_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, Employee> val = new T2<>(d(12.37), new Employee(d(12.34)));

        checkPutAll(shouldSucceed, cache, new T2<>(d(12.36), new Employee(d(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Employee(d(12.34)));
    }

    /** */
    private BigDecimal d(double val) {
        return BigDecimal.valueOf(val);
    }

    /** */
    private <K, V> void checkReplaceOps(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V> val, V okVal) {
        K k = val.get1();
        V v = val.get2();

        cache.put(k, okVal);

        CacheEntryProcessor<K, V, ?> entryProcessor = (e, arguments) -> {
            e.setValue((V)arguments[0]);

            return null;
        };

        Stream<Runnable> ops = Stream.of(
            () -> cache.replace(k, v),
            () -> cache.getAndReplace(k, v),
            () -> cache.replace(k, okVal, v),
            () -> cache.invoke(k, entryProcessor, v),
            () -> cache.replaceAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndReplaceAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.replaceAsync(k, okVal, v).get(FUT_TIMEOUT),
            () -> cache.invokeAsync(k, entryProcessor, v).get(FUT_TIMEOUT)
        );

        ops.forEach(checker);
    }

    /** */
    private <K, V> void checkPutOps(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V> val) {
        K k = val.get1();
        V v = val.get2();

        Stream<Runnable> ops = Stream.of(
            () -> cache.put(k, v),
            () -> cache.putIfAbsent(k, v),
            () -> cache.getAndPut(k, v),
            () -> cache.getAndPutIfAbsent(k, v),
            () -> cache.putAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.putIfAbsentAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndPutAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndPutIfAbsentAsync(k, v).get(FUT_TIMEOUT)
        );

        ops.forEach(checker);
    }

    /** */
    private <K, V> void checkPutAll(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V>... entries) {
        CacheEntryProcessor<K, V, ?> entryProcessor = (e, arguments) -> {
            e.setValue(((Iterator<V>)arguments[0]).next());

            return null;
        };

        Map<K, V> vals = Arrays.stream(entries).collect(Collectors.toMap(T2::get1, T2::get2));

        Stream<Runnable> ops = Stream.of(
            () -> cache.putAll(vals),
            () -> cache.putAllAsync(vals).get(FUT_TIMEOUT),
            () -> {
                Map<K, ? extends EntryProcessorResult<?>> map =
                    cache.invokeAll(vals.keySet(), entryProcessor, vals.values().iterator());

                for (EntryProcessorResult<?> result : map.values())
                    log.info(">>> " + result.get());
            },
            () -> {
                Map<K, ? extends EntryProcessorResult<?>> map =
                    cache.invokeAllAsync(vals.keySet(), entryProcessor, vals.values().iterator()).get(FUT_TIMEOUT);

                for (EntryProcessorResult<?> result : map.values())
                    log.info(">>> " + result.get());
            }
        );

        ops.forEach(checker);
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setQueryEntities(Collections.singletonList(qryEntity));

        if (TRANSACTIONAL_SNAPSHOT.equals(atomicityMode()))
            cache.setNearConfiguration(null);

        return cache;
    }

    /** */
    @NotNull protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** */
    @NotNull protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** */
    private static class Organization implements Serializable {
        /** Name. */
        private final String name;

        /**
         * @param name Name.
         */
        private Organization(String name) {
            this.name = name;
        }
    }

    /** */
    private static class Address implements Serializable {
        /** Name. */
        private final String address;

        /**
         * @param address Address.
         */
        private Address(String address) {
            this.address = address;
        }
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private static class DecOrganization implements Serializable {
        /** Id. */
        private final BigDecimal id;

        /**
         * @param id Id.
         */
        private DecOrganization(BigDecimal id) {
            this.id = id;
        }
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private static class Employee implements Serializable {
        /** Salary. */
        private final BigDecimal salary;

        /**
         * @param salary Salary.
         */
        private Employee(BigDecimal salary) {
            this.salary = salary;
        }
    }
}
