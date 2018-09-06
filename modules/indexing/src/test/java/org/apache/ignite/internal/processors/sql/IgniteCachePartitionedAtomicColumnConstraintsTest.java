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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IgniteCachePartitionedAtomicColumnConstraintsTest extends GridCommonAbstractTest {
    /** */
    private static final long FUT_TIMEOUT = 10_000L;

    /** */
    private static final String DEC_CACHE_NAME_FOR_PREC = "DEC_DEC_FOR_PREC";

    /** */
    private static final String DEC_ORG_CACHE_NAME_FOR_PREC = "DEC_ORG_FOR_PREC";

    /** */
    private static final String DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_PREC = "DEC_ORG_WITH_FIELDS_FOR_PREC";

    /** */
    private static final String OBJ_CACHE_NAME_FOR_PREC = "ORG_EMPLOYEE_FOR_PREC";

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
    private static final String DEC_ORG_CACHE_NAME_FOR_SCALE = "DEC_ORG_FOR_SCALE";

    /** */
    private static final String DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_SCALE = "DEC_ORG_WITH_FIELDS_FOR_SCALE";

    /** */
    private static final String OBJ_CACHE_SCALE_NAME_FOR_SCALE = "ORG_EMPLOYEE_FOR_SCALE";

    /** */
    private Consumer<Runnable> shouldFail = (op) -> assertThrowsWithCause(op, IgniteException.class);

    /** */
    private Consumer<Runnable> shouldSucceed = Runnable::run;

    /** @throws Exception If failed.*/
    private void createCacheForStringTest() throws Exception {
        Map<String, Integer> strStrPrecision = new HashMap<>();

        strStrPrecision.put(KEY_FIELD_NAME, 5);
        strStrPrecision.put(VAL_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), String.class.getName())
            .setFieldsPrecision(strStrPrecision)), STR_CACHE_NAME);

        Map<String, Integer> orgAddressPrecision = new HashMap<>();

        orgAddressPrecision.put("name", 5);
        orgAddressPrecision.put("address", 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(Organization.class.getName(), Address.class.getName())
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
    private void createCacheForDecPrecTest() throws Exception {
        Map<String, Integer> decDecPrecision = new HashMap<>();

        decDecPrecision.put(KEY_FIELD_NAME, 4);
        decDecPrecision.put(VAL_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), BigDecimal.class.getName())
            .setFieldsPrecision(decDecPrecision)), DEC_CACHE_NAME_FOR_PREC);

        Map<String, Integer> orgEmployeePrecision = new HashMap<>();

        orgEmployeePrecision.put("id", 4);
        orgEmployeePrecision.put("salary", 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(DecOrganization.class.getName(), Employee.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(orgEmployeePrecision)), OBJ_CACHE_NAME_FOR_PREC);

        Map<String, Integer> decOrgPrecision = new HashMap<>();

        decOrgPrecision.put(KEY_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), DecOrganization.class.getName())
            .setFieldsPrecision(decOrgPrecision)), DEC_ORG_CACHE_NAME_FOR_PREC);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), DecOrganization.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(decOrgPrecision)), DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_PREC);
    }

    /** @throws Exception If failed.*/
    private void createCacheForDecScaleTest() throws Exception {
        Map<String, Integer> decDecScale = new HashMap<>();
        Map<String, Integer> decDecPrecision = new HashMap<>();

        decDecPrecision.put(KEY_FIELD_NAME, 4);
        decDecPrecision.put(VAL_FIELD_NAME, 4);
        decDecScale.put(KEY_FIELD_NAME, 2);
        decDecScale.put(VAL_FIELD_NAME, 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), BigDecimal.class.getName())
            .setFieldsScale(decDecScale).setFieldsPrecision(decDecPrecision)), DEC_CACHE_NAME_FOR_SCALE);

        Map<String, Integer> orgEmployeeScale = new HashMap<>();
        Map<String, Integer> orgEmployeePrecision = new HashMap<>();

        orgEmployeePrecision.put("id", 4);
        orgEmployeePrecision.put("salary", 4);
        orgEmployeeScale.put("id", 2);
        orgEmployeeScale.put("salary", 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(DecOrganization.class.getName(), Employee.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsScale(orgEmployeeScale).setFieldsPrecision(orgEmployeePrecision)), OBJ_CACHE_SCALE_NAME_FOR_SCALE);

        Map<String, Integer> decOrgScale = new HashMap<>();
        Map<String, Integer> decOrgPrecision = new HashMap<>();

        decOrgScale.put(KEY_FIELD_NAME, 2);
        decOrgPrecision.put(KEY_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), DecOrganization.class.getName())
            .setFieldsScale(decOrgScale).setFieldsPrecision(decOrgPrecision)), DEC_ORG_CACHE_NAME_FOR_SCALE);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), DecOrganization.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsScale(decOrgScale).setFieldsPrecision(decOrgPrecision)), DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_SCALE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
        createCacheForStringTest();
        createCacheForDecPrecTest();
        createCacheForDecScaleTest();
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testPutTooLongStringKeyFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("123456", "2");

        checkPutAll(shouldFail, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testPutTooLongStringKeyFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("123456"), new Address("2"));

        checkPutAll(shouldFail, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongStringKeyFail2() throws Exception {
        doCheckPutTooLongStringKeyFail2(STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testPutLongStringKey() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("12345", "2");

        checkPutAll(shouldSucceed, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testPutLongStringKeyField() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("12345"), new Address("2"));

        checkPutAll(shouldSucceed, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongStringKey2() throws Exception {
        doCheckPutLongStringKey2(STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testPutTooLongDecimalValueFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(123.45));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(123.45), BigDecimal.valueOf(12.35));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalValueFieldFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_PREC);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.36));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(123.45)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyFieldFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_PREC);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(123.45));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyFail2() throws Exception {
        doCheckPutTooLongDecimalKeyFail2(DEC_ORG_CACHE_NAME_FOR_PREC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyFail3() throws Exception {
        doCheckPutTooLongDecimalKeyFail2(DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_PREC);
    }

    /**
     * @throws Exception If failed.
     */
    private void doCheckPutTooLongDecimalKeyFail2(String cacheName) {
        IgniteCache<BigDecimal, DecOrganization> cache = jcache(0, cacheName);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.34));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<BigDecimal, DecOrganization> val = new T2<>(BigDecimal.valueOf(123.45), org1);

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), org2), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalValue() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(12.37));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKey() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_PREC);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.37), BigDecimal.valueOf(12.35));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalValueField() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_PREC);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.36));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.37)));

        checkPutAll(shouldSucceed, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKeyField() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_NAME_FOR_PREC);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.37));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldSucceed, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKey2() throws Exception {
        doCheckPutValidDecimalKey2(DEC_ORG_CACHE_NAME_FOR_PREC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKey3() throws Exception {
        doCheckPutValidDecimalKey2(DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_PREC);
    }

    /**
     * @throws Exception If failed.
     */
    private void doCheckPutValidDecimalKey2(String cacheName) {
        IgniteCache<BigDecimal, DecOrganization> cache = jcache(0, cacheName);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.34));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<BigDecimal, DecOrganization> key2 = new T2<>(BigDecimal.valueOf(12.37), org1);

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), org2), key2);

        checkPutOps(shouldSucceed, cache, key2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalValueScaleFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(3.456));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyScaleFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(3.456), BigDecimal.valueOf(12.35));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalValueFieldScaleFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_SCALE_NAME_FOR_SCALE);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.36));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(3.456)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyFieldScaleFail() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_SCALE_NAME_FOR_SCALE);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(3.456));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyScaleFail2() throws Exception {
        doCheckPutTooLongDecimalKeyScaleFail2(DEC_ORG_CACHE_NAME_FOR_SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongDecimalKeyScaleFail3() throws Exception {
        doCheckPutTooLongDecimalKeyScaleFail2(DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    private void doCheckPutTooLongDecimalKeyScaleFail2(String cacheName) {
        IgniteCache<BigDecimal, DecOrganization> cache = jcache(0, cacheName);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.37));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<BigDecimal, DecOrganization> val = new T2<>(BigDecimal.valueOf(3.456), org1);

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), org2), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalValueScale() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(12.37));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKeyScale() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME_FOR_SCALE);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.37), BigDecimal.valueOf(12.35));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalValueFieldScale() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_SCALE_NAME_FOR_SCALE);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.36));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.37)));

        checkPutAll(shouldSucceed, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKeyFieldScale() throws Exception {
        IgniteCache<DecOrganization, Employee> cache = jcache(0, OBJ_CACHE_SCALE_NAME_FOR_SCALE);

        DecOrganization org1 = new DecOrganization(BigDecimal.valueOf(12.37));

        DecOrganization org2 = new DecOrganization(BigDecimal.valueOf(12.34));

        T2<DecOrganization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldSucceed, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKeyScale2() throws Exception {
        doCheckPutValidDecimalKeyScale2(DEC_ORG_CACHE_NAME_FOR_SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidDecimalKeyScale3() throws Exception {
        doCheckPutValidDecimalKeyScale2(DEC_ORG_WITH_FIELDS_CACHE_NAME_FOR_SCALE);
    }

    private void doCheckPutValidDecimalKeyScale2(String cacheName) {
        IgniteCache<BigDecimal, DecOrganization> cache = jcache(0, cacheName);

        T2<BigDecimal, DecOrganization> key2 = new T2<>(BigDecimal.valueOf(12.37), new DecOrganization(BigDecimal.valueOf(12.34)));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), new DecOrganization(BigDecimal.valueOf(12.34))), key2);

        checkPutOps(shouldSucceed, cache, key2);
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
    @SuppressWarnings("UnusedDeclaration")
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
    @SuppressWarnings("UnusedDeclaration")
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
