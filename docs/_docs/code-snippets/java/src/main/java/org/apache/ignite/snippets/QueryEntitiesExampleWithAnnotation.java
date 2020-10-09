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

import java.io.Serializable;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

public class QueryEntitiesExampleWithAnnotation {
    // tag::query-entity-annotation[]
    class Person implements Serializable {
        /** Indexed field. Will be visible to the SQL engine. */
        @QuerySqlField(index = true)
        private long id;

        /** Queryable field. Will be visible to the SQL engine. */
        @QuerySqlField
        private String name;

        /** Will NOT be visible to the SQL engine. */
        private int age;

        /**
         * Indexed field sorted in descending order. Will be visible to the SQL engine.
         */
        @QuerySqlField(index = true, descending = true)
        private float salary;
    }

    public static void main(String[] args) {
        Ignite ignite = Ignition.start();
        CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<Long, Person>();
        personCacheCfg.setName("Person");

        personCacheCfg.setIndexedTypes(Long.class, Person.class);
        IgniteCache<Long, Person> cache = ignite.createCache(personCacheCfg);
    }

    // end::query-entity-annotation[]
}
