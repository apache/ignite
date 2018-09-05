package org.apache.ignite.internal.processors.sql;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.T2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

public class IgniteCachePartitionedAtomicDecimalColumnPrecisionConstraintsTest extends IgniteCachePartitionedAtomicTest {

    /** */
    private static final String DEC_CACHE_NAME = "DEC_DEC";

    /** */
    private static final String DEC_ORG_CACHE_NAME = "DEC_ORG";

    /** */
    private static final String DEC_ORG_WITH_FIELDS_CACHE_NAME = "DEC_ORG_WITH_FIELDS";

    /** */
    private static final String OBJ_CACHE_NAME = "ORG_EMPLOYEE";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        Map<String, Integer> decDecPrecision = new HashMap<>();

        decDecPrecision.put(KEY_FIELD_NAME, 4);
        decDecPrecision.put(VAL_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), BigDecimal.class.getName())
            .setFieldsPrecision(decDecPrecision)), DEC_CACHE_NAME);

        Map<String, Integer> orgEmployeePrecision = new HashMap<>();

        orgEmployeePrecision.put("id", 4);
        orgEmployeePrecision.put("salary", 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(Organization.class.getName(), Employee.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(orgEmployeePrecision)), OBJ_CACHE_NAME);

        Map<String, Integer> decOrgPrecision = new HashMap<>();

        decOrgPrecision.put(KEY_FIELD_NAME, 4);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), Organization.class.getName())
            .setFieldsPrecision(decOrgPrecision)), DEC_ORG_CACHE_NAME);

        jcache(grid(0), cacheConfiguration(new QueryEntity(BigDecimal.class.getName(), Organization.class.getName())
            .addQueryField("id", "java.math.BigDecimal", "id")
            .addQueryField("salary", "java.math.BigDecimal", "salary")
            .setFieldsPrecision(decOrgPrecision)), DEC_ORG_WITH_FIELDS_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(123.45));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(123.45), BigDecimal.valueOf(12.35));

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFieldFail() throws Exception {
        IgniteCache<Organization, Employee> cache = jcache(0, OBJ_CACHE_NAME);

        Organization org1 = new Organization(BigDecimal.valueOf(12.36));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<Organization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(123.45)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Employee(BigDecimal.valueOf(12.34)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFieldFail() throws Exception {
        IgniteCache<Organization, Employee> cache = jcache(0, OBJ_CACHE_NAME);

        Organization org1 = new Organization(BigDecimal.valueOf(123.45));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<Organization, Employee> val = new T2<>(org1, new Employee(BigDecimal.valueOf(12.35)));

        checkPutAll(shouldFail, cache, new T2<>(org2, new Employee(BigDecimal.valueOf(12.34))), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail2() throws Exception {
        doCheckPutTooLongKeyFail2(DEC_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail3() throws Exception {
        doCheckPutTooLongKeyFail2(DEC_ORG_WITH_FIELDS_CACHE_NAME);
    }

    private void doCheckPutTooLongKeyFail2(String cacheName) {
        IgniteCache<BigDecimal, Organization> cache = jcache(0, cacheName);

        Organization org1 = new Organization(BigDecimal.valueOf(12.34));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<BigDecimal, Organization> val = new T2<>(BigDecimal.valueOf(123.45), org1);

        checkPutAll(shouldFail, cache, new T2<>(BigDecimal.valueOf(12.34), org2), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidValue() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.36), BigDecimal.valueOf(12.37));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidKey() throws Exception {
        IgniteCache<BigDecimal, BigDecimal> cache = jcache(0, DEC_CACHE_NAME);

        T2<BigDecimal, BigDecimal> val = new T2<>(BigDecimal.valueOf(12.37), BigDecimal.valueOf(12.35));

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), BigDecimal.valueOf(12.34)), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidValueField() throws Exception {
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
    public void testPutValidKeyField() throws Exception {
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
    public void testPutValidKey2() throws Exception {
        doCheckPutValidKey2(DEC_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutValidKey3() throws Exception {
        doCheckPutValidKey2(DEC_ORG_WITH_FIELDS_CACHE_NAME);
    }

    private void doCheckPutValidKey2(String cacheName) {
        IgniteCache<BigDecimal, Organization> cache = jcache(0, cacheName);

        Organization org1 = new Organization(BigDecimal.valueOf(12.34));

        Organization org2 = new Organization(BigDecimal.valueOf(12.34));

        T2<BigDecimal, Organization> key2 = new T2<>(BigDecimal.valueOf(12.37), org1);

        checkPutAll(shouldSucceed, cache, new T2<>(BigDecimal.valueOf(12.34), org2), key2);

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
