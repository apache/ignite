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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class CacheConfigP2PStartClient {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        try (Ignite ignite = Ignition.start(cfg)) {
            int nodes = ignite.cluster().nodes().size();

            if (nodes != 3)
                throw new Exception("Unexpected nodes number: " + nodes);

            CacheConfiguration<Integer, Organization1> ccfg1 = new CacheConfiguration<>();

            ccfg1.setName("cache1");

            ccfg1.setNodeFilter(new CacheAllNodesFilter());

            ccfg1.setIndexedTypes(Integer.class, Organization1.class);

            System.out.println("Create cache1.");

            IgniteCache<Integer, Organization1> cache1 = ignite.createCache(ccfg1);

            for (int i = 0; i < 500; i++)
                cache1.put(i, new Organization1("org-" + i));

            System.out.println("Sleep some time.");

            Thread.sleep(5000); // Sleep some time to wait when connection of p2p loader is closed.

            System.out.println("Create cache2.");

            CacheConfiguration<Integer, Organization2> ccfg2 = new CacheConfiguration<>();

            ccfg2.setName("cache2");

            ccfg2.setIndexedTypes(Integer.class, Organization1.class);

            IgniteCache<Integer, Organization2> cache2 = ignite.createCache(ccfg2);
        }
    }

    /**
     * Organization class.
     */
    private static class Organization1 implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private UUID id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create organization.
         *
         * @param name Organization name.
         */
        Organization1(String name) {
            id = UUID.randomUUID();

            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }

    /**
     * Organization class.
     */
    private static class Organization2 implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private UUID id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create organization.
         *
         * @param name Organization name.
         */
        Organization2(String name) {
            id = UUID.randomUUID();

            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }
}
