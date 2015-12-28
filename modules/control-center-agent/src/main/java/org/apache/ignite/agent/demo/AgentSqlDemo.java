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

package org.apache.ignite.agent.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.agent.AgentConfiguration;
import org.apache.ignite.agent.demo.model.Car;
import org.apache.ignite.agent.demo.model.CarKey;
import org.apache.ignite.agent.demo.model.Country;
import org.apache.ignite.agent.demo.model.CountryKey;
import org.apache.ignite.agent.demo.model.Department;
import org.apache.ignite.agent.demo.model.DepartmentKey;
import org.apache.ignite.agent.demo.model.Employee;
import org.apache.ignite.agent.demo.model.EmployeeKey;
import org.apache.ignite.agent.demo.model.Parking;
import org.apache.ignite.agent.demo.model.ParkingKey;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.log4j.Logger;

/**
 * Demo for SQL.
 *
 * Cache will be created and populated with data to query.
 */
public class AgentSqlDemo {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataDemo.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /** */
    private static final String EMPLOYEE_CACHE_NAME = "demo-employee";

    /** */
    private static final String CAR_CACHE_NAME = "demo-car";

    /** */
    private static final Random rnd = new Random();

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

    /** Counter for threads in pool. */
    private static final AtomicInteger THREAD_CNT = new AtomicInteger(0);

    /**
     * Configure cacheEmployee.
     *
     * @param name Cache name.
     */
    private static <K, V> CacheConfiguration<K, V> cacheEmployee(String name) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(name);

        // Configure cacheEmployee types.
        Collection<QueryEntity> queryEntities = new ArrayList<>();

        // COUNTRY.
        QueryEntity type = new QueryEntity();

        queryEntities.add(type);

        type.setKeyType(CountryKey.class.getName());
        type.setValueType(Country.class.getName());

        // Query fields for COUNTRY.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("countryName", "java.lang.String");

        type.setFields(qryFlds);

        // Indexes for COUNTRY.
        type.setIndexes(Collections.singletonList(new QueryIndex("id")));

        ccfg.setQueryEntities(queryEntities);

        // DEPARTMENT.
        type = new QueryEntity();

        queryEntities.add(type);

        type.setKeyType(DepartmentKey.class.getName());
        type.setValueType(Department.class.getName());

        // Query fields for DEPARTMENT.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("departmentId", "java.lang.Integer");
        qryFlds.put("departmentName", "java.lang.String");
        qryFlds.put("countryId", "java.lang.Integer");
        qryFlds.put("managerId", "java.lang.Integer");

        type.setFields(qryFlds);

        // Indexes for DEPARTMENT.
        type.setIndexes(Collections.singletonList(new QueryIndex("departmentId")));

        ccfg.setQueryEntities(queryEntities);

        // EMPLOYEE.
        type = new QueryEntity();

        queryEntities.add(type);

        type.setKeyType(EmployeeKey.class.getName());
        type.setValueType(Employee.class.getName());

        // Query fields for EMPLOYEE.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("employeeId", "java.lang.Integer");
        qryFlds.put("firstName", "java.lang.String");
        qryFlds.put("lastName", "java.lang.String");
        qryFlds.put("email", "java.lang.String");
        qryFlds.put("phoneNumber", "java.lang.String");
        qryFlds.put("hireDate", "java.sql.Date");
        qryFlds.put("job", "java.lang.String");
        qryFlds.put("salary", "java.lang.Double");
        qryFlds.put("managerId", "java.lang.Integer");
        qryFlds.put("departmentId", "java.lang.Integer");

        type.setFields(qryFlds);

        // Indexes for EMPLOYEE.
        Collection<QueryIndex> indexes = new ArrayList<>();

        indexes.add(new QueryIndex("employeeId"));
        indexes.add(new QueryIndex("salary", false));

        // Group indexes for EMPLOYEE.
        LinkedHashMap<String, Boolean> grpItems = new LinkedHashMap<>();

        grpItems.put("firstName", Boolean.FALSE);
        grpItems.put("lastName", Boolean.TRUE);

        QueryIndex grpIdx = new QueryIndex(grpItems, QueryIndexType.SORTED);

        grpIdx.setName("EMP_NAMES");

        indexes.add(grpIdx);

        type.setIndexes(indexes);

        ccfg.setQueryEntities(queryEntities);

        return ccfg;
    }

    /**
     * Configure cacheEmployee.
     *
     * @param name Cache name.
     */
    private static <K, V> CacheConfiguration<K, V> cacheCar(String name) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(name);

        // Configure cacheEmployee types.
        Collection<QueryEntity> queryEntities = new ArrayList<>();

        // CAR.
        QueryEntity type = new QueryEntity();

        queryEntities.add(type);

        type.setKeyType(CarKey.class.getName());
        type.setValueType(Car.class.getName());

        // Query fields for CAR.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("carId", "java.lang.Integer");
        qryFlds.put("parkingId", "java.lang.Integer");
        qryFlds.put("carName", "java.lang.String");

        type.setFields(qryFlds);

        // Indexes for CAR.
        type.setIndexes(Collections.singletonList(new QueryIndex("carId")));

        ccfg.setQueryEntities(queryEntities);

        // PARKING.
        type = new QueryEntity();

        queryEntities.add(type);

        type.setKeyType(ParkingKey.class.getName());
        type.setValueType(Parking.class.getName());

        // Query fields for PARKING.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("parkingId", "java.lang.Integer");
        qryFlds.put("parkingName", "java.lang.String");

        type.setFields(qryFlds);

        // Indexes for PARKING.
        type.setIndexes(Collections.singletonList(new QueryIndex("parkingId")));

        ccfg.setQueryEntities(queryEntities);

        return ccfg;
    }

    /**
     * @param val Value to round.
     * @param places Numbers after point.
     * @return Rounded value;
     */
    private static double round(double val, int places) {
        if (places < 0)
            throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);

        val *= factor;

        long tmp = Math.round(val);

        return (double) tmp / factor;
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param range Time range in milliseconds.
     */
    private static void populateCacheEmployee(Ignite ignite, String name, long range) {
        log.trace("DEMO: Start population cache: '" + name + "' with data...");

        IgniteCache<CountryKey, Country> cacheCountry = ignite.cache(name);

        for (int i = 0; i < CNTR_CNT; i++)
            cacheCountry.put(new CountryKey(i), new Country(i, "State " + (i + 1)));

        IgniteCache<DepartmentKey, Department> cacheDepartment = ignite.cache(name);

        for (int i = 0; i < DEP_CNT; i++) {
            Integer mgrId = (i == 0 || rnd.nextBoolean()) ? null : rnd.nextInt(i);

            cacheDepartment.put(new DepartmentKey(i),
                new Department(i, "Department " + (i + 1), rnd.nextInt(CNTR_CNT), mgrId));
        }

        IgniteCache<EmployeeKey, Employee> cacheEmployee = ignite.cache(name);

        for (int i = 0; i < EMPL_CNT; i++) {
            Integer mgrId = (i == 0 || rnd.nextBoolean()) ? null : rnd.nextInt(i);

            double r = rnd.nextDouble();

            cacheEmployee.put(new EmployeeKey(i),
                new Employee(i, "first name " + (i + 1), "last name " + (i + 1), "email " + (i + 1),
                    "phone number " + (i + 1), new java.sql.Date((long)(r * range)), "job " + (i + 1),
                    round(r * 5000, 2) , mgrId, rnd.nextInt(DEP_CNT)));
        }

        log.trace("DEMO: Finished population cache: '" + name + "' with data.");
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     */
    private static void populateCacheCar(Ignite ignite, String name) {
        log.trace("DEMO: Start population cache: '" + name + "' with data...");

        IgniteCache<ParkingKey, Parking> cacheParking = ignite.cache(name);

        for (int i = 0; i < PARK_CNT; i++)
            cacheParking.put(new ParkingKey(i), new Parking(i, "Parking " + (i + 1)));

        IgniteCache<CarKey, Car> cacheCar = ignite.cache(name);

        for (int i = 0; i < CAR_CNT; i++)
            cacheCar.put(new CarKey(i), new Car(i, rnd.nextInt(PARK_CNT), "Car " + (i + 1)));


        log.trace("DEMO: Finished population cache: '" + name + "' with data.");
    }

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute periodically.
     *
     * @param corePoolSize Number of threads to keep in the pool, even if they are idle.
     * @param threadName Part of thread name that would be used by thread factory.
     * @return Newly created scheduled thread pool.
     */
    private static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, final String threadName) {
        ScheduledExecutorService srvc = Executors.newScheduledThreadPool(corePoolSize, new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, String.format("%s-%d", threadName, THREAD_CNT.getAndIncrement()));

                thread.setDaemon(true);

                return thread;
            }
        });

        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) srvc;

        // Setting up shutdown policy.
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        return srvc;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param ignite Ignite.
     * @param n - maximum count read/write key
     */
    private static void startLoad(final Ignite ignite, final int n) {
        final long diff = new java.util.Date().getTime();

        populateCacheEmployee(ignite, EMPLOYEE_CACHE_NAME, diff);

        populateCacheCar(ignite, CAR_CACHE_NAME);

        ScheduledExecutorService cachePool = newScheduledThreadPool(2, "demo-sql-load-cache-tasks");

        cachePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    IgniteCache<EmployeeKey, Employee> cache = ignite.cache(EMPLOYEE_CACHE_NAME);

                    if (cache != null)
                        for (int i = 0; i < n; i++) {
                            Integer employeeId = rnd.nextInt(EMPL_CNT);

                            Integer mgrId = (i == 0 || rnd.nextBoolean()) ? null : rnd.nextInt(employeeId);

                            double r = rnd.nextDouble();

                            cache.put(new EmployeeKey(employeeId),
                                new Employee(employeeId, "first name " + (i + 1), "last name " + (i + 1),
                                    "email " + (i + 1), "phone number " + (i + 1),
                                    new java.sql.Date((long)(r * diff)), "job " + (i + 1),
                                    round(r * 5000, 2), mgrId, rnd.nextInt(DEP_CNT)));

                            if (rnd.nextBoolean())
                                cache.remove(new EmployeeKey(rnd.nextInt(EMPL_CNT)));
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

        cachePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    IgniteCache<CarKey, Car> cache = ignite.cache(CAR_CACHE_NAME);

                    if (cache != null)
                        for (int i = 0; i < n; i++) {
                            Integer carId = rnd.nextInt(CAR_CNT);

                            cache.put(new CarKey(carId), new Car(carId, rnd.nextInt(PARK_CNT), "Car " + (i + 1)));

                            if (rnd.nextBoolean())
                                cache.remove(new CarKey(rnd.nextInt(CAR_CNT)));
                        }
                }
                catch (IllegalStateException ignored) {
                }
                catch (Throwable e) {
                    if (!e.getMessage().contains("cache is stopped"))
                        ignite.log().error("Cache write task execution error", e);
                }
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static boolean testDrive(AgentConfiguration acfg) {
        if (initLatch.compareAndSet(false, true)) {
            log.info("DEMO: Starting embedded node for sql test-drive...");

            try {
                IgniteConfiguration cfg = new IgniteConfiguration();

                cfg.setLocalHost("127.0.0.1");

                cfg.setMetricsLogFrequency(0);

                cfg.setGridLogger(new NullLogger());

                // Configure discovery SPI.
                TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

                TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

                ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500..47501"));

                discoSpi.setIpFinder(ipFinder);

                cfg.setDiscoverySpi(discoSpi);

                cfg.setCacheConfiguration(cacheEmployee(EMPLOYEE_CACHE_NAME), cacheCar(CAR_CACHE_NAME));

                log.trace("DEMO: Start embedded node with indexed enabled caches...");

                IgniteEx ignite = (IgniteEx)Ignition.start(cfg);

                String host = ((Collection<String>)
                    ignite.localNode().attribute(IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS)).iterator().next();

                Integer port = ignite.localNode().attribute(IgniteNodeAttributes.ATTR_REST_JETTY_PORT);

                if (F.isEmpty(host) || port == null) {
                    log.error("DEMO: Failed to start embedded node with rest!");

                    return false;
                }

                acfg.nodeUri(String.format("http://%s:%d", "0.0.0.0".equals(host) ? "127.0.0.1" : host, port));

                log.info("DEMO: Embedded node for sql test-drive successfully started");

                startLoad(ignite, 20);
            }
            catch (Exception e) {
                log.error("DEMO: Failed to start embedded node for sql test-drive!", e);

                return false;
            }
        }

        return true;
    }
}
