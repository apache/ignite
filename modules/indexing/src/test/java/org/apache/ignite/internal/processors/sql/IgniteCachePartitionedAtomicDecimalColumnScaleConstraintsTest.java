package org.apache.ignite.internal.processors.sql;

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

import javax.cache.processor.EntryProcessorResult;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

public class IgniteCachePartitionedAtomicDecimalColumnScaleConstraintsTest extends IgniteCachePartitionedAtomicTest {

    /** */
    private static final String DEC_CACHE_NAME = "DEC_DEC";

    /** */
    private static final String DEC_ORG_CACHE_NAME = "DEC_ORG";

    /** */
    private static final String DEC_ORG_WITH_FIELDS_CACHE_NAME = "DEC_ORG_WITH_FIELDS";

    /** */
    private static final String OBJ_CACHE_NAME = "ORG_EMPLOYEE";

    /** */
    private Consumer<Runnable> shouldFail = (op) -> assertThrowsWithCause(op, IgniteException.class);

    /** */
    private Consumer<Runnable> shouldSucceed = Runnable::run;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        Map<String, Integer> decDecScale = new HashMap<>();
        Map<String, Integer> decDecPrecision = new HashMap<>();

        decDecPrecision.put(KEY_FIELD_NAME, 4);
        decDecPrecision.put(VAL_FIELD_NAME, 4);
        decDecScale.put(KEY_FIELD_NAME, 2);
        decDecScale.put(VAL_FIELD_NAME, 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), BigDecimal.class.getName())
            .setFieldsScale(decDecScale).setFieldsPrecision(decDecPrecision)), DEC_CACHE_NAME);

        Map<String, Integer> orgEmployeeScale = new HashMap<>();
        Map<String, Integer> orgEmployeePrecision = new HashMap<>();

        orgEmployeePrecision.put("id", 4);
        orgEmployeePrecision.put("salary", 4);
        orgEmployeeScale.put("id", 2);
        orgEmployeeScale.put("salary", 2);

        jcache(grid(0), cacheConfiguration(new QueryEntity(Organization.class.getName(), Employee.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsScale(orgEmployeeScale).setFieldsPrecision(orgEmployeePrecision)), OBJ_CACHE_NAME);

        Map<String, Integer> decOrgScale = new HashMap<>();
        Map<String, Integer> decOrgPrecision = new HashMap<>();

        decOrgScale.put(KEY_FIELD_NAME, 2);
        decOrgPrecision.put(KEY_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), Organization.class.getName())
            .setFieldsScale(decOrgScale).setFieldsPrecision(decOrgPrecision)), DEC_ORG_CACHE_NAME);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), Organization.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsScale(decOrgScale).setFieldsPrecision(decOrgPrecision)), DEC_ORG_WITH_FIELDS_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueScaleFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(3.456));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyScaleFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(3.456), BigDecimal.valueOf(12.35));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFieldScaleFail() throws Exception {
        IgniteCache<Organization, Employee> cache = jcache(0, OBJ_CACHE_NAME);

        Organization org1 = new Organization(BigDecimal.valueOf(12.36));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<Organization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(3.456)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFieldScaleFail() throws Exception {
        IgniteCache<Organization, Employee> cache = jcache(0, OBJ_CACHE_NAME);

        Organization org1 = new Organization(BigDecimal.valueOf(3.456));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<Organization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyScaleFail2() throws Exception {
        doCheckPutTooLongKeyScaleFail2(DEC_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyScaleFail3() throws Exception {
        doCheckPutTooLongKeyScaleFail2(DEC_ORG_WITH_FIELDS_CACHE_NAME);
    }

    private void doCheckPutTooLongKeyScaleFail2(String cacheName) {
        IgniteCache<BigDecimal, Organization> cache = jcache(0, cacheName);

        Organization org1 = new Organization(BigDecimal.valueOf(12.37));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<BigDecimal, Organization> val = new T2<>(BigDecimal.valueOf(3.456), org1);

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), org2), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidValueScale() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(12.37));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidKeyScale() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.37), BigDecimal.valueOf(12.35));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidValueFieldScale() throws Exception {
        IgniteCache<Organization, Employee> cache = jcache(0, OBJ_CACHE_NAME);

        Organization org1 = new Organization(BigDecimal.valueOf(12.36));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<Organization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.37)));

        checkPutAll(shouldSucceed, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidKeyFieldScale() throws Exception {
        IgniteCache<Organization, Employee> cache = jcache(0, OBJ_CACHE_NAME);

        Organization org1 = new Organization(BigDecimal.valueOf(12.37));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<Organization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldSucceed, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidKeyScale2() throws Exception {
        doCheckPutValidKeyScale2(DEC_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidKeyScale3() throws Exception {
        doCheckPutValidKeyScale2(DEC_ORG_WITH_FIELDS_CACHE_NAME);
    }

    private void doCheckPutValidKeyScale2(String cacheName) {
        IgniteCache<BigDecimal, Organization> cache = jcache(0, cacheName);

        T2<BigDecimal, Organization> key2 = new T2<>(BigDecimal.valueOf(12.37), new Organization(BigDecimal.valueOf(12.34)));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), new Organization(BigDecimal.valueOf(12.34))), key2);

        checkPutOps(shouldSucceed, cache, key2);
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** Id. */
        private final BigDecimal id;

        /**
         * @param id Id.
         */
        private Organization(BigDecimal id) {
            this.id = id;
        }
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private static class Employee implements Serializable {
        /** salary. */
        private final BigDecimal salary;

        /**
         * @param salary Salary.
         */
        private Employee(BigDecimal salary) {
            this.salary = salary;
        }
    }

}

