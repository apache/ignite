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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Based on Yardstick benchmark.
 */
public class IgniteCacheOffheapTieredMultithreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int RANGE = 1_000_000;

    /** */
    private static IgniteCache<Integer, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration<?,?> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(ATOMIC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setBackups(1);
        cacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cacheCfg.setIndexedTypes(
            Integer.class, Person.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);

        cache = grid(0).cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryPut() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while(!end.get()) {
                    if (rnd.nextInt(5) == 0) {
                        double salary = rnd.nextDouble() * RANGE * 1000;

                        double maxSalary = salary + 1000;

                        Collection<Cache.Entry<Integer, Object>> entries = executeQuery(salary, maxSalary);

                        for (Cache.Entry<Integer, Object> entry : entries) {
                            Person p = (Person)entry.getValue();

                            if (p.getSalary() < salary || p.getSalary() > maxSalary)
                                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                                    ", person=" + p + ']');
                        }
                    }
                    else {
                        int i = rnd.nextInt(RANGE);

                        cache.put(i, new Person(i, "firstName" + i, "lastName" + i, i * 1000));
                    }
                }

                return null;
            }
        }, 64);

        Thread.sleep(30 * 1000);

        end.set(true);

        fut.get();
    }



    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private Collection<Cache.Entry<Integer, Object>> executeQuery(double minSalary, double maxSalary) throws Exception {
        SqlQuery qry = new SqlQuery(Person.class, "salary >= ? and salary <= ?");

        qry.setArgs(minSalary, maxSalary);

        return cache.query(qry).getAll();
    }

    /**
     * Person record used for query test.
     */
    public static class Person implements Externalizable {
        /** Person ID. */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary. */
        @QuerySqlField(index = true)
        private double salary;

        /**
         * Constructs empty person.
         */
        public Person() {
            // No-op.
        }

        /**
         * Constructs person record that is not linked to any organization.
         *
         * @param id Person ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        public Person(int id, String firstName, String lastName, double salary) {
            this(id, 0, firstName, lastName, salary);
        }

        /**
         * Constructs person record.
         *
         * @param id Person ID.
         * @param orgId Organization ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        public Person(int id, int orgId, String firstName, String lastName, double salary) {
            this.id = id;
            this.orgId = orgId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
        }

        /**
         * @return Person id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Person id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Organization id.
         */
        public int getOrganizationId() {
            return orgId;
        }

        /**
         * @param orgId Organization id.
         */
        public void setOrganizationId(int orgId) {
            this.orgId = orgId;
        }

        /**
         * @return Person first name.
         */
        public String getFirstName() {
            return firstName;
        }

        /**
         * @param firstName Person first name.
         */
        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        /**
         * @return Person last name.
         */
        public String getLastName() {
            return lastName;
        }

        /**
         * @param lastName Person last name.
         */
        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        /**
         * @return Salary.
         */
        public double getSalary() {
            return salary;
        }

        /**
         * @param salary Salary.
         */
        public void setSalary(double salary) {
            this.salary = salary;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(id);
            out.writeInt(orgId);
            out.writeUTF(firstName);
            out.writeUTF(lastName);
            out.writeDouble(salary);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = in.readInt();
            orgId = in.readInt();
            firstName = in.readUTF();
            lastName = in.readUTF();
            salary = in.readDouble();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || (o instanceof Person) && id == ((Person)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", id=" + id +
                ", orgId=" + orgId +
                ", lastName=" + lastName +
                ", salary=" + salary +
                ']';
        }
    }
}