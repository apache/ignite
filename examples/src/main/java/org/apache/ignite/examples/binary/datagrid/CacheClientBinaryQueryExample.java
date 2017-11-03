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

package org.apache.ignite.examples.binary.datagrid;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Employee;
import org.apache.ignite.examples.model.EmployeeKey;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.OrganizationType;
import org.apache.ignite.binary.BinaryObject;

/**
 * This example demonstrates use of binary objects with cache queries.
 * The example populates cache with sample data and runs several SQL and full text queries over this data.
 * <p>
 * Remote nodes should always be started with the following command:
 * {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}
 * <p>
 * Alternatively you can run {@link org.apache.ignite.examples.ExampleNodeStartup} in another JVM which will
 * start a node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheClientBinaryQueryExample {
    /** Organization cache name. */
    private static final String ORGANIZATION_CACHE_NAME = CacheClientBinaryQueryExample.class.getSimpleName()
        + "Organizations";

    /** Employee cache name. */
    private static final String EMPLOYEE_CACHE_NAME = CacheClientBinaryQueryExample.class.getSimpleName()
        + "Employees";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Binary objects cache query example started.");

            CacheConfiguration<Integer, Organization> orgCacheCfg = new CacheConfiguration<>();

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            orgCacheCfg.setName(ORGANIZATION_CACHE_NAME);

            orgCacheCfg.setQueryEntities(Arrays.asList(createOrganizationQueryEntity()));

            CacheConfiguration<EmployeeKey, Employee> employeeCacheCfg = new CacheConfiguration<>();

            employeeCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            employeeCacheCfg.setName(EMPLOYEE_CACHE_NAME);

            employeeCacheCfg.setQueryEntities(Arrays.asList(createEmployeeQueryEntity()));

            employeeCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EmployeeKey.class));

            try (IgniteCache<Integer, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg);
                 IgniteCache<EmployeeKey, Employee> employeeCache = ignite.getOrCreateCache(employeeCacheCfg)
            ) {
                if (ignite.cluster().forDataNodes(orgCache.getName()).nodes().isEmpty()) {
                    System.out.println();
                    System.out.println(">>> This example requires remote cache nodes to be started.");
                    System.out.println(">>> Please start at least 1 remote cache node.");
                    System.out.println(">>> Refer to example's javadoc for details on configuration.");
                    System.out.println();

                    return;
                }

                // Populate cache with sample data entries.
                populateCache(orgCache, employeeCache);

                // Get cache that will work with binary objects.
                IgniteCache<BinaryObject, BinaryObject> binaryCache = employeeCache.withKeepBinary();

                // Run SQL query example.
                sqlQuery(binaryCache);

                // Run SQL query with join example.
                sqlJoinQuery(binaryCache);

                // Run SQL fields query example.
                sqlFieldsQuery(binaryCache);

                // Run full text query example.
                textQuery(binaryCache);

                System.out.println();
            }
            finally {
                // Delete caches with their content completely.
                ignite.destroyCache(ORGANIZATION_CACHE_NAME);
                ignite.destroyCache(EMPLOYEE_CACHE_NAME);
            }
        }
    }

    /**
     * Create cache type metadata for {@link Employee}.
     *
     * @return Cache type metadata.
     */
    private static QueryEntity createEmployeeQueryEntity() {
        QueryEntity employeeEntity = new QueryEntity();

        employeeEntity.setValueType(Employee.class.getName());
        employeeEntity.setKeyType(EmployeeKey.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("name", String.class.getName());
        fields.put("salary", Long.class.getName());
        fields.put("addr.zip", Integer.class.getName());
        fields.put("organizationId", Integer.class.getName());
        fields.put("addr.street", Integer.class.getName());

        employeeEntity.setFields(fields);

        employeeEntity.setIndexes(Arrays.asList(
            new QueryIndex("name"),
            new QueryIndex("salary"),
            new QueryIndex("addr.zip"),
            new QueryIndex("organizationId"),
            new QueryIndex("addr.street", QueryIndexType.FULLTEXT)
        ));

        return employeeEntity;
    }

    /**
     * Create cache type metadata for {@link Organization}.
     *
     * @return Cache type metadata.
     */
    private static QueryEntity createOrganizationQueryEntity() {
        QueryEntity organizationEntity = new QueryEntity();

        organizationEntity.setValueType(Organization.class.getName());
        organizationEntity.setKeyType(Integer.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("name", String.class.getName());
        fields.put("address.street", String.class.getName());

        organizationEntity.setFields(fields);

        organizationEntity.setIndexes(Arrays.asList(
            new QueryIndex("name")
        ));

        return organizationEntity;
    }

    /**
     * Queries employees that have provided ZIP code in address.
     *
     * @param cache Ignite cache.
     */
    private static void sqlQuery(IgniteCache<BinaryObject, BinaryObject> cache) {
        SqlQuery<BinaryObject, BinaryObject> query = new SqlQuery<>(Employee.class, "zip = ?");

        int zip = 94109;

        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> employees = cache.query(query.setArgs(zip));

        System.out.println();
        System.out.println(">>> Employees with zip " + zip + ':');

        for (Cache.Entry<BinaryObject, BinaryObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue().deserialize());
    }

    /**
     * Queries employees that work for organization with provided name.
     *
     * @param cache Ignite cache.
     */
    private static void sqlJoinQuery(IgniteCache<BinaryObject, BinaryObject> cache) {
        SqlQuery<BinaryObject, BinaryObject> qry = new SqlQuery<>(Employee.class,
            "from Employee, \"" + ORGANIZATION_CACHE_NAME + "\".Organization as org " +
                "where Employee.organizationId = org._key and org.name = ?");

        String organizationName = "GridGain";

        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> employees =
            cache.query(qry.setArgs(organizationName));

        System.out.println();
        System.out.println(">>> Employees working for " + organizationName + ':');

        for (Cache.Entry<BinaryObject, BinaryObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue());
    }

    /**
     * Queries names and salaries for all employees.
     *
     * @param cache Ignite cache.
     */
    private static void sqlFieldsQuery(IgniteCache<BinaryObject, BinaryObject> cache) {
        SqlFieldsQuery qry = new SqlFieldsQuery("select name, salary from Employee");

        QueryCursor<List<?>> employees = cache.query(qry);

        System.out.println();
        System.out.println(">>> Employee names and their salaries:");

        for (List<?> row : employees.getAll())
            System.out.println(">>>     [Name=" + row.get(0) + ", salary=" + row.get(1) + ']');
    }

    /**
     * Queries employees that live in Texas using full-text query API.
     *
     * @param cache Ignite cache.
     */
    private static void textQuery(IgniteCache<BinaryObject, BinaryObject> cache) {
        TextQuery<BinaryObject, BinaryObject> qry = new TextQuery<>(Employee.class, "TX");

        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> employees = cache.query(qry);

        System.out.println();
        System.out.println(">>> Employees living in Texas:");

        for (Cache.Entry<BinaryObject, BinaryObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue().deserialize());
    }

    /**
     * Populates cache with data.
     *
     * @param orgCache Organization cache.
     * @param employeeCache Employee cache.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static void populateCache(IgniteCache<Integer, Organization> orgCache,
        IgniteCache<EmployeeKey, Employee> employeeCache) {
        orgCache.put(1, new Organization(
            "GridGain",
            new Address("1065 East Hillsdale Blvd, Foster City, CA", 94404),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis())
        ));

        orgCache.put(2, new Organization(
            "Microsoft",
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis())
        ));

        employeeCache.put(new EmployeeKey(1, 1), new Employee(
            "James Wilson",
            12500,
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            Arrays.asList("Human Resources", "Customer Service")
        ));

        employeeCache.put(new EmployeeKey(2, 1), new Employee(
            "Daniel Adams",
            11000,
            new Address("184 Fidler Drive, San Antonio, TX", 78130),
            Arrays.asList("Development", "QA")
        ));

        employeeCache.put(new EmployeeKey(3, 1), new Employee(
            "Cristian Moss",
            12500,
            new Address("667 Jerry Dove Drive, Florence, SC", 29501),
            Arrays.asList("Logistics")
        ));

        employeeCache.put(new EmployeeKey(4, 2), new Employee(
            "Allison Mathis",
            25300,
            new Address("2702 Freedom Lane, San Francisco, CA", 94109),
            Arrays.asList("Development")
        ));

        employeeCache.put(new EmployeeKey(5, 2), new Employee(
            "Breana Robbin",
            6500,
            new Address("3960 Sundown Lane, Austin, TX", 78130),
            Arrays.asList("Sales")
        ));

        employeeCache.put(new EmployeeKey(6, 2), new Employee(
            "Philip Horsley",
            19800,
            new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
            Arrays.asList("Sales")
        ));

        employeeCache.put(new EmployeeKey(7, 2), new Employee(
            "Brian Peters",
            10600,
            new Address("1407 Pearlman Avenue, Boston, MA", 12110),
            Arrays.asList("Development", "QA")
        ));
    }
}

