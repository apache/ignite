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

package org.apache.ignite.tests.p2p.startcache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.tests.p2p.cache.Person;

import static org.junit.Assert.assertEquals;

/** */
public class PojoCollectionComputeTest {
    /** */
    private void run(String taskName) {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setPeerClassLoadingEnabled(true)
            .setClientMode(true)
            .setLocalHost("127.0.0.1")
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))));

        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteRunnable task;

            if (PojoArrayTask.class.getSimpleName().equals(taskName))
                task = new PojoArrayTask();
            else if (PojoListTask.class.getSimpleName().equals(taskName))
                task = new PojoListTask();
            else
                task = new PojoMapTask();

            ignite.compute().run(task);
        }
    }

    /** */
    private static class PojoArrayTask implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        @Override public void run() {
            IgniteCache<Integer, Object[]> cache = ignite.getOrCreateCache("personArrayCache");

            Object[] objs = new Object[1];
            objs[0] = new Person("name0");

            cache.put(0, objs);

            Object[] get = cache.get(0);
            assertEquals(objs[0], get[0]);
        }
    }

    /** */
    private static class PojoListTask implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        @Override public void run() {
            IgniteCache<Integer, List<Person>> personListCache = ignite.getOrCreateCache("personListCache");

            List<Person> persons = new ArrayList<>();
            persons.add(new Person("name0"));

            personListCache.put(0, persons);

            List<Person> get = personListCache.get(0);
            assertEquals(persons.get(0), get.get(0));
        }
    }

    /** */
    private static class PojoMapTask implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        @Override public void run() {
            IgniteCache<Person, Map<Person, Person>> personMapCache = ignite.getOrCreateCache("personMapCache");

            Map<Person, Person> persons = new HashMap<>();
            persons.put(new Person("nameValKey"), new Person("nameValVal"));

            personMapCache.put(new Person("nameKey"), persons);

            Map<Person, Person> get = personMapCache.get(new Person("nameKey"));
            assertEquals(persons, get);
        }
    }

    /** */
    public static void main(String[] args) {
        new PojoCollectionComputeTest().run(args[0]);
    }
}
