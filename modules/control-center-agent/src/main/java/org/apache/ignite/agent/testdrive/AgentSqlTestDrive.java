/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.agent.testdrive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.agent.AgentConfiguration;
import org.apache.ignite.agent.testdrive.model.Car;
import org.apache.ignite.agent.testdrive.model.CarKey;
import org.apache.ignite.agent.testdrive.model.Country;
import org.apache.ignite.agent.testdrive.model.CountryKey;
import org.apache.ignite.agent.testdrive.model.Department;
import org.apache.ignite.agent.testdrive.model.DepartmentKey;
import org.apache.ignite.agent.testdrive.model.Employee;
import org.apache.ignite.agent.testdrive.model.EmployeeKey;
import org.apache.ignite.agent.testdrive.model.Parking;
import org.apache.ignite.agent.testdrive.model.ParkingKey;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Test drive for SQL.
 *
 * Cache will be created and populated with data to query.
 */
public class AgentSqlTestDrive {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataTestDrive.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /** */
    private static final String EMPLOYEE_CACHE_NAME = "test-drive-employee";

    /** */
    private static final String CAR_CACHE_NAME = "test-drive-car";

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

    /**
     * Configure cacheEmployee.
     *
     * @param name Cache name.
     */
    private static <K, V> CacheConfiguration<K, V> cacheEmployee(String name) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(name);

        // Configure cacheEmployee types.
        Collection<CacheTypeMetadata> meta = new ArrayList<>();

        // COUNTRY.
        CacheTypeMetadata type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(CountryKey.class.getName());
        type.setValueType(Country.class.getName());

        // Query fields for COUNTRY.
        Map<String, Class<?>> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", int.class);
        qryFlds.put("countryName", String.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for COUNTRY.
        Map<String, Class<?>> ascFlds = new LinkedHashMap<>();

        ascFlds.put("id", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // DEPARTMENT.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(DepartmentKey.class.getName());
        type.setValueType(Department.class.getName());

        // Query fields for DEPARTMENT.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("departmentId", int.class);
        qryFlds.put("departmentName", String.class);
        qryFlds.put("countryId", Integer.class);
        qryFlds.put("managerId", Integer.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for DEPARTMENT.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("departmentId", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // EMPLOYEE.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(EmployeeKey.class.getName());
        type.setValueType(Employee.class.getName());

        // Query fields for EMPLOYEE.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("employeeId", int.class);
        qryFlds.put("firstName", String.class);
        qryFlds.put("lastName", String.class);
        qryFlds.put("email", String.class);
        qryFlds.put("phoneNumber", String.class);
        qryFlds.put("hireDate", java.sql.Date.class);
        qryFlds.put("job", String.class);
        qryFlds.put("salary", Double.class);
        qryFlds.put("managerId", Integer.class);
        qryFlds.put("departmentId", Integer.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for EMPLOYEE.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("employeeId", int.class);

        type.setAscendingFields(ascFlds);

        // Desc fields for EMPLOYEE.
        Map<String, Class<?>> descFlds = new LinkedHashMap<>();

        descFlds.put("salary", Double.class);

        type.setDescendingFields(descFlds);

        // Groups for EMPLOYEE.
        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = new LinkedHashMap<>();

        LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> grpItems = new LinkedHashMap<>();

        grpItems.put("firstName", new IgniteBiTuple<Class<?>, Boolean>(String.class, false));
        grpItems.put("lastName", new IgniteBiTuple<Class<?>, Boolean>(String.class, true));

        grps.put("EMP_NAMES", grpItems);

        type.setGroups(grps);

        ccfg.setTypeMetadata(meta);

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
        Collection<CacheTypeMetadata> meta = new ArrayList<>();

        // CAR.
        CacheTypeMetadata type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(CarKey.class.getName());
        type.setValueType(Car.class.getName());

        // Query fields for CAR.
        Map<String, Class<?>> qryFlds = new LinkedHashMap<>();

        qryFlds.put("carId", int.class);
        qryFlds.put("parkingId", int.class);
        qryFlds.put("carName", String.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for CAR.
        Map<String, Class<?>> ascFlds = new LinkedHashMap<>();

        ascFlds.put("carId", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

        // PARKING.
        type = new CacheTypeMetadata();

        meta.add(type);

        type.setKeyType(ParkingKey.class.getName());
        type.setValueType(Parking.class.getName());

        // Query fields for PARKING.
        qryFlds = new LinkedHashMap<>();

        qryFlds.put("parkingId", int.class);
        qryFlds.put("parkingName", String.class);

        type.setQueryFields(qryFlds);

        // Ascending fields for PARKING.
        ascFlds = new LinkedHashMap<>();

        ascFlds.put("parkingId", int.class);

        type.setAscendingFields(ascFlds);

        ccfg.setTypeMetadata(meta);

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
     */
    private static void populateCacheEmployee(Ignite ignite, String name) {
        log.log(Level.FINE, "TEST-DRIVE-SQL: Start population cache: '" + name + "' with data...");

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

        long off = java.sql.Date.valueOf("2007-01-01").getTime();

        long end = java.sql.Date.valueOf("2016-01-01").getTime();

        long diff = end - off + 1;

        for (int i = 0; i < EMPL_CNT; i++) {
            Integer mgrId = (i == 0 || rnd.nextBoolean()) ? null : rnd.nextInt(i);

            double r = rnd.nextDouble();

            cacheEmployee.put(new EmployeeKey(i),
                new Employee(i, "first name " + (i + 1), "last name " + (i + 1), "email " + (i + 1),
                    "phone number " + (i + 1), new java.sql.Date(off + (long)(r * diff)), "job " + (i + 1),
                    round(r * 5000, 2) , mgrId, rnd.nextInt(DEP_CNT)));
        }

        log.log(Level.FINE, "TEST-DRIVE-SQL: Finished population cache: '" + name + "' with data.");
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     */
    private static void populateCacheCar(Ignite ignite, String name) {
        log.log(Level.FINE, "TEST-DRIVE-SQL: Start population cache: '" + name + "' with data...");

        IgniteCache<ParkingKey, Parking> cacheParking = ignite.cache(name);

        for (int i = 0; i < PARK_CNT; i++)
            cacheParking.put(new ParkingKey(i), new Parking(i, "Parking " + (i + 1)));

        IgniteCache<CarKey, Car> cacheDepartment = ignite.cache(name);

        for (int i = 0; i < CAR_CNT; i++)
            cacheDepartment.put(new CarKey(i), new Car(i, rnd.nextInt(PARK_CNT), "Car " + (i + 1)));


        log.log(Level.FINE, "TEST-DRIVE-SQL: Finished population cache: '" + name + "' with data.");
    }

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static void testDrive(AgentConfiguration acfg) {
        if (initLatch.compareAndSet(false, true)) {
            log.log(Level.INFO, "TEST-DRIVE-SQL: Starting embedded node for sql test-drive...");

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

                log.log(Level.FINE, "TEST-DRIVE-SQL: Start embedded node with indexed enabled caches...");

                IgniteEx ignite = (IgniteEx)Ignition.start(cfg);

                String host = ((Collection<String>)
                    ignite.localNode().attribute(IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS)).iterator().next();

                Integer port = ignite.localNode().attribute(IgniteNodeAttributes.ATTR_REST_JETTY_PORT);

                if (F.isEmpty(host) || port == null) {
                    log.log(Level.SEVERE, "TEST-DRIVE-SQL: Failed to start embedded node with rest!");

                    return;
                }

                acfg.nodeUri(String.format("http://%s:%d", "0.0.0.0".equals(host) ? "127.0.0.1" : host, port));

                log.log(Level.INFO, "TEST-DRIVE-SQL: Embedded node for sql test-drive successfully started");

                populateCacheEmployee(ignite, EMPLOYEE_CACHE_NAME);

                populateCacheCar(ignite, CAR_CACHE_NAME);
            }
            catch (Exception e) {
                log.log(Level.SEVERE, "TEST-DRIVE-SQL: Failed to start embedded node for sql test-drive!", e);
            }
        }
    }
}
