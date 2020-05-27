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

package org.apache.ignite.internal.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 *
 */
public class IgniteApplication {
    /** Config path. */
    public static final String CONFIG_PATH = "/mnt/client_app/ignite-client-config.xml";

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> cfgs = IgnitionEx.loadConfiguration(CONFIG_PATH);
        IgniteConfiguration cfg = cfgs.get1();

        cfg.setClientMode(true);

        System.out.println("Starting Ignite client...");

        try (Ignite ign = Ignition.start(cfg)) {
            System.out.println("Creating cache...");

            IgniteCache<Integer, Integer> cache = ign.createCache("test-cache");

            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            executeSql(cache, "CREATE TABLE person(id INT, fio VARCHAR, PRIMARY KEY(id))");
            executeSql(cache, "INSERT INTO person(id, fio) VALUES(?, ?)", 1, "Ivanov Ivan");
            executeSql(cache, "INSERT INTO person(id, fio) VALUES(?, ?)", 2, "Petrov Petr");
            executeSql(cache, "INSERT INTO person(id, fio) VALUES(?, ?)", 3, "Sidorov Sidr");

            System.out.println("Ignite Client Finish.");
        }
    }

    /**
     * @param cache Cache.
     * @param query Query.
     * @param args Args.
     */
    private static void executeSql(IgniteCache<Integer, Integer> cache, String query, Object... args) {
        cache.query(new SqlFieldsQuery(query).setArgs(args)).getAll();
    }
}
