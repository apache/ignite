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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;

//tag::collocation[]
public class AffinityCollocationExample {

    static class Person {
        private int id;
        private String companyId;
        private String name;

        public Person(int id, String companyId, String name) {
            this.id = id;
            this.companyId = companyId;
            this.name = name;
        }

        public int getId() {
            return id;
        }
    }

    static class PersonKey {
        private int id;

        @AffinityKeyMapped
        private String companyId;

        public PersonKey(int id, String companyId) {
            this.id = id;
            this.companyId = companyId;
        }
    }

    static class Company {
        private String id;
        private String name;

        public Company(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }
    }

    public void configureAffinityKeyWithAnnotation() {
        CacheConfiguration<PersonKey, Person> personCfg = new CacheConfiguration<PersonKey, Person>("persons");
        personCfg.setBackups(1);

        CacheConfiguration<String, Company> companyCfg = new CacheConfiguration<>("companies");
        companyCfg.setBackups(1);

        try (Ignite ignite = Ignition.start()) {
            IgniteCache<PersonKey, Person> personCache = ignite.getOrCreateCache(personCfg);
            IgniteCache<String, Company> companyCache = ignite.getOrCreateCache(companyCfg);

            Company c1 = new Company("company1", "My company");
            Person p1 = new Person(1, c1.getId(), "John");

            // Both the p1 and c1 objects will be cached on the same node
            personCache.put(new PersonKey(p1.getId(), c1.getId()), p1);
            companyCache.put("company1", c1);

            // Get the person object
            p1 = personCache.get(new PersonKey(1, "company1"));
        }
    }

    // tag::affinity-key-class[]
    public void configureAffinitKeyWithAffinityKeyClass() {
        CacheConfiguration<AffinityKey<Integer>, Person> personCfg = new CacheConfiguration<AffinityKey<Integer>, Person>(
                "persons");
        personCfg.setBackups(1);

        CacheConfiguration<String, Company> companyCfg = new CacheConfiguration<String, Company>("companies");
        companyCfg.setBackups(1);

        Ignite ignite = Ignition.start();

        IgniteCache<AffinityKey<Integer>, Person> personCache = ignite.getOrCreateCache(personCfg);
        IgniteCache<String, Company> companyCache = ignite.getOrCreateCache(companyCfg);

        Company c1 = new Company("company1", "My company");
        Person p1 = new Person(1, c1.getId(), "John");

        // Both the p1 and c1 objects will be cached on the same node
        personCache.put(new AffinityKey<Integer>(p1.getId(), c1.getId()), p1);
        companyCache.put(c1.getId(), c1);

        // Get the person object
        p1 = personCache.get(new AffinityKey(1, "company1"));
    }

    // end::affinity-key-class[]
    // tag::config-with-key-configuration[]
    public void configureAffinityKeyWithCacheKeyConfiguration() {
        CacheConfiguration<PersonKey, Person> personCfg = new CacheConfiguration<PersonKey, Person>("persons");
        personCfg.setBackups(1);

        // Configure the affinity key
        personCfg.setKeyConfiguration(new CacheKeyConfiguration("Person", "companyId"));

        CacheConfiguration<String, Company> companyCfg = new CacheConfiguration<String, Company>("companies");
        companyCfg.setBackups(1);

        Ignite ignite = Ignition.start();

        IgniteCache<PersonKey, Person> personCache = ignite.getOrCreateCache(personCfg);
        IgniteCache<String, Company> companyCache = ignite.getOrCreateCache(companyCfg);

        Company c1 = new Company("company1", "My company");
        Person p1 = new Person(1, c1.getId(), "John");

        // Both the p1 and c1 objects will be cached on the same node
        personCache.put(new PersonKey(1, c1.getId()), p1);
        companyCache.put(c1.getId(), c1);

        // Get the person object
        p1 = personCache.get(new PersonKey(1, "company1"));
    }
    // end::config-with-key-configuration[]
}
//end::collocation[]
