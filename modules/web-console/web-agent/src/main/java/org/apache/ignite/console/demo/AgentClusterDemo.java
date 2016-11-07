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

package org.apache.ignite.console.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.demo.model.Car;
import org.apache.ignite.console.demo.model.Country;
import org.apache.ignite.console.demo.model.Department;
import org.apache.ignite.console.demo.model.Employee;
import org.apache.ignite.console.demo.model.Parking;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Demo for cluster features like SQL and Monitoring.
 *
 * Cache will be created and populated with data to query.
 */
public class AgentClusterDemo {
    /** */
    private static final Logger log = Logger.getLogger(AgentClusterDemo.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /** */
    private static final int NODE_CNT = 3;

    /** */
    private static final String COUNTRY_CACHE_NAME = "CountryCache";

    /** */
    private static final String DEPARTMENT_CACHE_NAME = "DepartmentCache";

    /** */
    private static final String EMPLOYEE_CACHE_NAME = "EmployeeCache";

    /** */
    private static final String PARKING_CACHE_NAME = "ParkingCache";

    /** */
    private static final String CAR_CACHE_NAME = "CarCache";

    /** */
    private static final Set<String> DEMO_CACHES = new HashSet<>(Arrays.asList(COUNTRY_CACHE_NAME,
        DEPARTMENT_CACHE_NAME, EMPLOYEE_CACHE_NAME, PARKING_CACHE_NAME, CAR_CACHE_NAME));

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
     * Create base cache configuration.
     *
     * @param name cache name.
     * @return Cache configuration with basic properties set.
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfiguration(String name) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(name);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
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

    /**
     * Configure node.
     * @param gridIdx Grid name index.
     * @param client If {@code true} then start client node.
     * @return IgniteConfiguration
     */
    private static  IgniteConfiguration igniteConfiguration(int gridIdx, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName((client ? "demo-server-" : "demo-client-") + gridIdx);
        cfg.setLocalHost("127.0.0.1");
        cfg.setIncludeEventTypes(EVTS_DISCOVERY);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:60900.." + (60900 + NODE_CNT - 1)));

        // Configure discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setLocalPort(60900);
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);
        commSpi.setLocalPort(60800);

        cfg.setCommunicationSpi(commSpi);
        cfg.setGridLogger(new Log4JLogger(log));
        cfg.setMetricsLogFrequency(0);
        cfg.getConnectorConfiguration().setPort(60700);

        if (client)
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(cacheCountry(), cacheDepartment(), cacheEmployee(), cacheParking(), cacheCar());

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * @param val Value to round.
     * @param places Numbers after point.
     * @return Rounded value;
     */
    private static double round(double val, int places) {
        if (places < 0)
            throw new IllegalArgumentException();

        long factor = (long)Math.pow(10, places);

        val *= factor;

        long tmp = Math.round(val);

        return (double)tmp / factor;
    }

    /**
     * @param ignite Ignite.
     * @param range Time range in milliseconds.
     */
    private static void populateCacheEmployee(Ignite ignite, long range) {
        if (log.isDebugEnabled())
            log.debug("DEMO: Start employees population with data...");

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
                new java.sql.Date((long)(r * range)), "Job manager #" + n, 1000 + round(r * 4000, 2)));
        }

        for (int i = 0, n = 1; i < EMPL_CNT; i++, n++) {
            Integer depId = rnd.nextInt(DEP_CNT);

            double r = rnd.nextDouble();

            cacheEmployee.put(i, new Employee(i, depId, depId, "First name employee #" + n,
                "Last name employee #" + n, "Email employee #" + n, "Phone number employee #" + n,
                new java.sql.Date((long)(r * range)), "Job employee #" + n, 500 + round(r * 2000, 2)));
        }

        if (log.isDebugEnabled())
            log.debug("DEMO: Finished employees population.");
    }

    /**
     * @param ignite Ignite.
     */
    private static void populateCacheCar(Ignite ignite) {
        if (log.isDebugEnabled())
            log.debug("DEMO: Start cars population...");

        IgniteCache<Integer, Parking> cacheParking = ignite.cache(PARKING_CACHE_NAME);

        for (int i = 0, n = 1; i < PARK_CNT; i++, n++)
            cacheParking.put(i, new Parking(i, "Parking #" + n, n * 10));

        IgniteCache<Integer, Car> cacheCar = ignite.cache(CAR_CACHE_NAME);

        for (int i = 0, n = 1; i < CAR_CNT; i++, n++)
            cacheCar.put(i, new Car(i, rnd.nextInt(PARK_CNT), "Car #" + n));

        if (log.isDebugEnabled())
            log.debug("DEMO: Finished cars population.");
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

        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)srvc;

        // Setting up shutdown policy.
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        return srvc;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param ignite Ignite.
     * @param cnt - maximum count read/write key
     */
    private static void startLoad(final Ignite ignite, final int cnt) {
        final long diff = new java.util.Date().getTime();

        populateCacheEmployee(ignite, diff);
        populateCacheCar(ignite);

        ScheduledExecutorService cachePool = newScheduledThreadPool(2, "demo-sql-load-cache-tasks");

        cachePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    for (String cacheName : ignite.cacheNames()) {
                        if (!DEMO_CACHES.contains(cacheName)) {
                            IgniteCache<Integer, String> otherCache = ignite.cache(cacheName);

                            if (otherCache != null) {
                                for (int i = 0, n = 1; i < cnt; i++, n++) {
                                    Integer key = rnd.nextInt(1000);

                                    String val = otherCache.get(key);

                                    if (val == null)
                                        otherCache.put(key, "other-" + key);
                                    else if (rnd.nextInt(100) < 30)
                                        otherCache.remove(key);
                                }
                            }
                        }
                    }

                    IgniteCache<Integer, Employee> cacheEmployee = ignite.cache(EMPLOYEE_CACHE_NAME);

                    if (cacheEmployee != null)
                        try(Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            for (int i = 0, n = 1; i < cnt; i++, n++) {
                                Integer id = rnd.nextInt(EMPL_CNT);

                                Integer depId = rnd.nextInt(DEP_CNT);

                                double r = rnd.nextDouble();

                                cacheEmployee.put(id, new Employee(id, depId, depId, "First name employee #" + n,
                                    "Last name employee #" + n, "Email employee #" + n, "Phone number employee #" + n,
                                    new java.sql.Date((long)(r * diff)), "Job employee #" + n, 500 + round(r * 2000, 2)));

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
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static boolean testDrive(AgentConfiguration acfg) {
        if (initLatch.compareAndSet(false, true)) {
            log.info("DEMO: Starting embedded nodes for demo...");

            System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "1");
            System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");
            System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");

            System.setProperty(IGNITE_JETTY_PORT, "60800");
            System.setProperty(IGNITE_NO_ASCII, "true");

            try {
                IgniteEx ignite = (IgniteEx)Ignition.start(igniteConfiguration(0, false));

                final AtomicInteger cnt = new AtomicInteger(0);

                final ScheduledExecutorService execSrv = Executors.newSingleThreadScheduledExecutor();

                execSrv.scheduleAtFixedRate(new Runnable() {
                    @Override public void run() {
                        int idx = cnt.incrementAndGet();

                        try {
                            Ignition.start(igniteConfiguration(idx, idx == NODE_CNT));
                        }
                        catch (Throwable e) {
                            log.error("DEMO: Failed to start embedded node: " + e.getMessage());
                        }
                        finally {
                            if (idx == NODE_CNT)
                                execSrv.shutdown();
                        }
                    }
                }, 10, 10, TimeUnit.SECONDS);

                if (log.isDebugEnabled())
                    log.debug("DEMO: Started embedded nodes with indexed enabled caches...");

                Collection<String> jettyAddrs = ignite.localNode().attribute(ATTR_REST_JETTY_ADDRS);

                String host = jettyAddrs == null ? null : jettyAddrs.iterator().next();

                Integer port = ignite.localNode().attribute(ATTR_REST_JETTY_PORT);

                if (F.isEmpty(host) || port == null) {
                    log.error("DEMO: Failed to start embedded node with rest!");

                    return false;
                }

                acfg.demoNodeUri(String.format("http://%s:%d", host, port));

                log.info("DEMO: Embedded nodes for sql and monitoring demo successfully started");

                startLoad(ignite, 20);
            }
            catch (Exception e) {
                log.error("DEMO: Failed to start embedded node for sql and monitoring demo!", e);

                return false;
            }
        }

        return true;
    }
}
