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

package org.apache.ignite.examples.portable.datagrid;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.portable.Address;
import org.apache.ignite.examples.portable.Employee;
import org.apache.ignite.examples.portable.EmployeeKey;
import org.apache.ignite.examples.portable.ExamplePortableNodeStartup;
import org.apache.ignite.examples.portable.Organization;
import org.apache.ignite.examples.portable.OrganizationType;
import org.apache.ignite.portable.PortableObject;

/**
 * This example demonstrates use of portable objects with cache queries.
 * The example populates cache with sample data and runs several SQL and full text queries over this data.
 * <p>
 * Remote nodes should always be started with {@link ExamplePortableNodeStartup} which starts a node with
 * {@code examples/config/portable/example-ignite-portable.xml} configuration.
 */
public class CacheClientPortableQueryExample {
    /** Organization cache name. */
    private static final String ORGANIZATION_CACHE_NAME = CacheClientPortableQueryExample.class.getSimpleName()
        + "Organizations";

    /** Employee cache name. */
    private static final String EMPLOYEE_CACHE_NAME = CacheClientPortableQueryExample.class.getSimpleName()
        + "Employees";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/portable/example-ignite-portable.xml")) {
            System.out.println();
            System.out.println(">>> Portable objects cache query example started.");

            CacheConfiguration<Integer, Organization> orgCacheCfg = new CacheConfiguration<>();

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            orgCacheCfg.setName(ORGANIZATION_CACHE_NAME);

            orgCacheCfg.setTypeMetadata(Arrays.asList(createOrganizationTypeMetadata()));

            CacheConfiguration<EmployeeKey, Employee> employeeCacheCfg = new CacheConfiguration<>();

            employeeCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            employeeCacheCfg.setName(EMPLOYEE_CACHE_NAME);

            employeeCacheCfg.setTypeMetadata(Arrays.asList(createEmployeeTypeMetadata()));

            try (IgniteCache<Integer, Organization> orgCache = ignite.createCache(orgCacheCfg);
                 IgniteCache<EmployeeKey, Employee> employeeCache = ignite.createCache(employeeCacheCfg)
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

                // Get cache that will work with portable objects.
                IgniteCache<PortableObject, PortableObject> portableCache = employeeCache.withKeepPortable();

                // Run SQL query example.
                sqlQuery(portableCache);

                // Run SQL query with join example.
                sqlJoinQuery(portableCache);

                // Run SQL fields query example.
                sqlFieldsQuery(portableCache);

                // Run full text query example.
                textQuery(portableCache);

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
    private static CacheTypeMetadata createEmployeeTypeMetadata() {
        CacheTypeMetadata employeeTypeMeta = new CacheTypeMetadata();

        employeeTypeMeta.setValueType(Employee.class);

        employeeTypeMeta.setKeyType(EmployeeKey.class);

        Map<String, Class<?>> ascFields = new HashMap<>();

        ascFields.put("name", String.class);
        ascFields.put("salary", Long.class);
        ascFields.put("address.zip", Integer.class);
        ascFields.put("organizationId", Integer.class);

        employeeTypeMeta.setAscendingFields(ascFields);

        employeeTypeMeta.setTextFields(Arrays.asList("address.street"));

        return employeeTypeMeta;
    }

    /**
     * Create cache type metadata for {@link Organization}.
     *
     * @return Cache type metadata.
     */
    private static CacheTypeMetadata createOrganizationTypeMetadata() {
        CacheTypeMetadata organizationTypeMeta = new CacheTypeMetadata();

        organizationTypeMeta.setValueType(Organization.class);

        organizationTypeMeta.setKeyType(Integer.class);

        Map<String, Class<?>> ascFields = new HashMap<>();

        ascFields.put("name", String.class);

        Map<String, Class<?>> queryFields = new HashMap<>();

        queryFields.put("address.street", String.class);

        organizationTypeMeta.setAscendingFields(ascFields);

        organizationTypeMeta.setQueryFields(queryFields);

        return organizationTypeMeta;
    }

    /**
     * Queries employees that have provided ZIP code in address.
     *
     * @param cache Ignite cache.
     */
    private static void sqlQuery(IgniteCache<PortableObject, PortableObject> cache) {
        SqlQuery<PortableObject, PortableObject> query = new SqlQuery<>(Employee.class, "zip = ?");

        int zip = 94109;

        QueryCursor<Cache.Entry<PortableObject, PortableObject>> employees = cache.query(query.setArgs(zip));

        System.out.println();
        System.out.println(">>> Employees with zip " + zip + ':');

        for (Cache.Entry<PortableObject, PortableObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue().deserialize());
    }

    /**
     * Queries employees that work for organization with provided name.
     *
     * @param cache Ignite cache.
     */
    private static void sqlJoinQuery(IgniteCache<PortableObject, PortableObject> cache) {
        SqlQuery<PortableObject, PortableObject> query = new SqlQuery<>(Employee.class,
            "from Employee, \"" + ORGANIZATION_CACHE_NAME + "\".Organization as org " +
                "where Employee.organizationId = org._key and org.name = ?");

        String organizationName = "GridGain";

        QueryCursor<Cache.Entry<PortableObject, PortableObject>> employees =
            cache.query(query.setArgs(organizationName));

        System.out.println();
        System.out.println(">>> Employees working for " + organizationName + ':');

        for (Cache.Entry<PortableObject, PortableObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue());
    }

    /**
     * Queries names and salaries for all employees.
     *
     * @param cache Ignite cache.
     */
    private static void sqlFieldsQuery(IgniteCache<PortableObject, PortableObject> cache) {
        SqlFieldsQuery query = new SqlFieldsQuery("select name, salary from Employee");

        QueryCursor<List<?>> employees = cache.query(query);

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
    private static void textQuery(IgniteCache<PortableObject, PortableObject> cache) {
        TextQuery<PortableObject, PortableObject> query = new TextQuery<>(Employee.class, "TX");

        QueryCursor<Cache.Entry<PortableObject, PortableObject>> employees = cache.query(query);

        System.out.println();
        System.out.println(">>> Employees living in Texas:");

        for (Cache.Entry<PortableObject, PortableObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue().deserialize());
    }

    /**
     * Populates cache with data.
     *
     * @param orgCache Organization cache.
     * @param employeeCache Employee cache.
     */
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

