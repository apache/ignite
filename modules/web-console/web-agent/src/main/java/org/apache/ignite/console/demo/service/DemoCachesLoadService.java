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

package org.apache.ignite.console.demo.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.demo.AgentDemoUtils;
import org.apache.ignite.console.demo.model.Car;
import org.apache.ignite.console.demo.model.Country;
import org.apache.ignite.console.demo.model.Department;
import org.apache.ignite.console.demo.model.Employee;
import org.apache.ignite.console.demo.model.Parking;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Demo service. Create and populate caches. Run demo load on caches.
 */
public class DemoCachesLoadService implements Service {
    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Thread pool to execute cache load operations. */
    private ScheduledExecutorService cachePool;

    /** */
    private static final String COUNTRY_CACHE_NAME = "CountryCache";

    /** */
    private static final String DEPARTMENT_CACHE_NAME = "DepartmentCache";

    /** */
    private static final String EMPLOYEE_CACHE_NAME = "EmployeeCache";

    /** */
    private static final String PARKING_CACHE_NAME = "ParkingCache";

    /** */
    public static final String CAR_CACHE_NAME = "CarCache";

    /** */
    static final Set<String> DEMO_CACHES = new HashSet<>(Arrays.asList(COUNTRY_CACHE_NAME,
        DEPARTMENT_CACHE_NAME, EMPLOYEE_CACHE_NAME, PARKING_CACHE_NAME, CAR_CACHE_NAME));

    /** Countries count. */
    private static final int CNTR_CNT = 10;

    /** Departments count */
    private static final int DEP_CNT = 100;

    /** Employees count. */
    private static final int EMPL_CNT = 1000;

    /** Countries count. */
    private static final int CAR_CNT = 100;

    /** Departments count */
    private static final int PARK_CNT = 10;

    /** */
    private static final Random rnd = new Random();

    /** Maximum count read/write key. */
    private final int cnt;

    /** Time range in milliseconds. */
    private final long range;

    /**
     * @param cnt Maximum count read/write key.
     */
    public DemoCachesLoadService(int cnt) {
        this.cnt = cnt;

        range = new java.util.Date().getTime();
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        if (cachePool != null)
            cachePool.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        ignite.createCache(cacheCountry());
        ignite.createCache(cacheDepartment());
        ignite.createCache(cacheEmployee());
        ignite.createCache(cacheCar());
        ignite.createCache(cacheParking());

        populateCacheEmployee();
        populateCacheCar();

        cachePool = AgentDemoUtils.newScheduledThreadPool(2, "demo-sql-load-cache-tasks");
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        cachePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    IgniteCache<Integer, Employee> cacheEmployee = ignite.cache(EMPLOYEE_CACHE_NAME);

                    if (cacheEmployee != null)
                        try(Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            for (int i = 0, n = 1; i < cnt; i++, n++) {
                                Integer id = rnd.nextInt(EMPL_CNT);

                                Integer depId = rnd.nextInt(DEP_CNT);

                                double r = rnd.nextDouble();

                                cacheEmployee.put(id, new Employee(id, depId, depId, "First name employee #" + n,
                                    "Last name employee #" + n, "Email employee #" + n, "Phone number employee #" + n,
                                    new java.sql.Date((long)(r * range)), "Job employee #" + n,
                                    500 + AgentDemoUtils.round(r * 2000, 2)));

                                if (rnd.nextBoolean())
                                    cacheEmployee.remove(rnd.nextInt(EMPL_CNT));

                                cacheEmployee.get(rnd.nextInt(EMPL_CNT));
                            }

                            if (rnd.nextInt(100) > 20)
                                tx.commit();
                        }
                }
                catch (Throwable e) {
                    if (!e.getMessage().contains("cache is stopped"))
                        ignite.log().error("Cache write task execution error", e);
                }
            }
        }, 10, 3, TimeUnit.SECONDS);

        cachePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    IgniteCache<Integer, Car> cache = ignite.cache(CAR_CACHE_NAME);

                    if (cache != null)
                        for (int i = 0; i < cnt; i++) {
                            Integer carId = rnd.nextInt(CAR_CNT);

                            cache.put(carId, new Car(carId, rnd.nextInt(PARK_CNT), "Car #" + (i + 1)));

                            if (rnd.nextBoolean())
                                cache.remove(rnd.nextInt(CAR_CNT));
                        }
                }
                catch (IllegalStateException ignored) {
                    // No-op.
                }
                catch (Throwable e) {
                    if (!e.getMessage().contains("cache is stopped"))
                        ignite.log().error("Cache write task execution error", e);
                }
            }
        }, 10, 3, TimeUnit.SECONDS);
    }


    /**
     * Create base cache configuration.
     *
     * @param name cache name.
     * @return Cache configuration with basic properties set.
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfiguration(String name) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(name);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setQueryDetailMetricsSize(10);
        ccfg.setStartSize(100);
        ccfg.setStatisticsEnabled(true);

        return ccfg;
    }

    /**
     * Configure cacheCountry.
     */
    private static <K, V> CacheConfiguration<K, V> cacheCountry() {
        CacheConfiguration<K, V> ccfg = cacheConfiguration(COUNTRY_CACHE_NAME);

        // Configure cacheCountry types.
        Collection<QueryEntity> qryEntities = new ArrayList<>();

        // COUNTRY.
        QueryEntity type = new QueryEntity();

        qryEntities.add(type);

        type.setKeyType(Integer.class.getName());
        type.setValueType(Country.class.getName());

        // Query fields for COUNTRY.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("name", "java.lang.String");
        qryFlds.put("population", "java.lang.Integer");

        type.setFields(qryFlds);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * Configure cacheEmployee.
     */
    private static <K, V> CacheConfiguration<K, V> cacheDepartment() {
        CacheConfiguration<K, V> ccfg = cacheConfiguration(DEPARTMENT_CACHE_NAME);

        // Configure cacheDepartment types.
        Collection<QueryEntity> qryEntities = new ArrayList<>();

        // DEPARTMENT.
        QueryEntity type = new QueryEntity();

        qryEntities.add(type);

        type.setKeyType(Integer.class.getName());
        type.setValueType(Department.class.getName());

        // Query fields for DEPARTMENT.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("countryId", "java.lang.Integer");
        qryFlds.put("name", "java.lang.String");

        type.setFields(qryFlds);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * Configure cacheEmployee.
     */
    private static <K, V> CacheConfiguration<K, V> cacheEmployee() {
        CacheConfiguration<K, V> ccfg = cacheConfiguration(EMPLOYEE_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        // Configure cacheEmployee types.
        Collection<QueryEntity> qryEntities = new ArrayList<>();

        // EMPLOYEE.
        QueryEntity type = new QueryEntity();

        qryEntities.add(type);

        type.setKeyType(Integer.class.getName());
        type.setValueType(Employee.class.getName());

        // Query fields for EMPLOYEE.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("departmentId", "java.lang.Integer");
        qryFlds.put("managerId", "java.lang.Integer");
        qryFlds.put("firstName", "java.lang.String");
        qryFlds.put("lastName", "java.lang.String");
        qryFlds.put("email", "java.lang.String");
        qryFlds.put("phoneNumber", "java.lang.String");
        qryFlds.put("hireDate", "java.sql.Date");
        qryFlds.put("job", "java.lang.String");
        qryFlds.put("salary", "java.lang.Double");

        type.setFields(qryFlds);

        // Indexes for EMPLOYEE.
        Collection<QueryIndex> indexes = new ArrayList<>();

        QueryIndex idx = new QueryIndex();

        idx.setName("EMP_NAMES");
        idx.setIndexType(QueryIndexType.SORTED);
        LinkedHashMap<String, Boolean> indFlds = new LinkedHashMap<>();

        indFlds.put("firstName", Boolean.FALSE);
        indFlds.put("lastName", Boolean.FALSE);

        idx.setFields(indFlds);

        indexes.add(idx);
        indexes.add(new QueryIndex("salary", QueryIndexType.SORTED, false, "EMP_SALARY"));

        type.setIndexes(indexes);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * Configure cacheEmployee.
     */
    private static <K, V> CacheConfiguration<K, V> cacheParking() {
        CacheConfiguration<K, V> ccfg = cacheConfiguration(PARKING_CACHE_NAME);

        // Configure cacheParking types.
        Collection<QueryEntity> qryEntities = new ArrayList<>();

        // PARKING.
        QueryEntity type = new QueryEntity();

        qryEntities.add(type);

        type.setKeyType(Integer.class.getName());
        type.setValueType(Parking.class.getName());

        // Query fields for PARKING.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("name", "java.lang.String");
        qryFlds.put("capacity", "java.lang.Integer");

        type.setFields(qryFlds);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * Configure cacheEmployee.
     */
    private static <K, V> CacheConfiguration<K, V> cacheCar() {
        CacheConfiguration<K, V> ccfg = cacheConfiguration(CAR_CACHE_NAME);

        // Configure cacheCar types.
        Collection<QueryEntity> qryEntities = new ArrayList<>();

        // CAR.
        QueryEntity type = new QueryEntity();

        qryEntities.add(type);

        type.setKeyType(Integer.class.getName());
        type.setValueType(Car.class.getName());

        // Query fields for CAR.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("parkingId", "java.lang.Integer");
        qryFlds.put("name", "java.lang.String");

        type.setFields(qryFlds);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /** */
    private void populateCacheEmployee() {
        if (ignite.log().isDebugEnabled())
            ignite.log().debug("DEMO: Start employees population with data...");

        IgniteCache<Integer, Country> cacheCountry = ignite.cache(COUNTRY_CACHE_NAME);

        for (int i = 0, n = 1; i < CNTR_CNT; i++, n++)
            cacheCountry.put(i, new Country(i, "Country #" + n, n * 10000000));

        IgniteCache<Integer, Department> cacheDepartment = ignite.cache(DEPARTMENT_CACHE_NAME);

        IgniteCache<Integer, Employee> cacheEmployee = ignite.cache(EMPLOYEE_CACHE_NAME);

        for (int i = 0, n = 1; i < DEP_CNT; i++, n++) {
            cacheDepartment.put(i, new Department(n, rnd.nextInt(CNTR_CNT), "Department #" + n));

            double r = rnd.nextDouble();

            cacheEmployee.put(i, new Employee(i, rnd.nextInt(DEP_CNT), null, "First name manager #" + n,
                "Last name manager #" + n, "Email manager #" + n, "Phone number manager #" + n,
                new java.sql.Date((long)(r * range)), "Job manager #" + n, 1000 + AgentDemoUtils.round(r * 4000, 2)));
        }

        for (int i = 0, n = 1; i < EMPL_CNT; i++, n++) {
            Integer depId = rnd.nextInt(DEP_CNT);

            double r = rnd.nextDouble();

            cacheEmployee.put(i, new Employee(i, depId, depId, "First name employee #" + n,
                "Last name employee #" + n, "Email employee #" + n, "Phone number employee #" + n,
                new java.sql.Date((long)(r * range)), "Job employee #" + n, 500 + AgentDemoUtils.round(r * 2000, 2)));
        }

        if (ignite.log().isDebugEnabled())
            ignite.log().debug("DEMO: Finished employees population.");
    }

    /** */
    private void populateCacheCar() {
        if (ignite.log().isDebugEnabled())
            ignite.log().debug("DEMO: Start cars population...");

        IgniteCache<Integer, Parking> cacheParking = ignite.cache(PARKING_CACHE_NAME);

        for (int i = 0, n = 1; i < PARK_CNT; i++, n++)
            cacheParking.put(i, new Parking(i, "Parking #" + n, n * 10));

        IgniteCache<Integer, Car> cacheCar = ignite.cache(CAR_CACHE_NAME);

        for (int i = 0, n = 1; i < CAR_CNT; i++, n++)
            cacheCar.put(i, new Car(i, rnd.nextInt(PARK_CNT), "Car #" + n));

        if (ignite.log().isDebugEnabled())
            ignite.log().debug("DEMO: Finished cars population.");
    }
}
