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

package org.apache.ignite.internal.ducktest;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class SampleDataStreamerApplication extends IgniteAwareApplication {
    /**
     * @param ignite Ignite.
     */
    public SampleDataStreamerApplication(Ignite ignite) {
        super(ignite);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @param args Args.
     */
    private static void executeSql(IgniteCache<Integer, Integer> cache, String qry, Object... args) {
        cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /** {@inheritDoc} */
    @Override protected void run(String[] args) {
        System.out.println("Creating cache...");

        IgniteCache<Integer, Integer> cache = ignite.createCache(args[0]);

        for (int i = 0; i < Integer.parseInt(args[1]); i++)
            cache.put(i, i);

        executeSql(cache, "CREATE TABLE person(id INT, fio VARCHAR, PRIMARY KEY(id))");
        executeSql(cache, "INSERT INTO person(id, fio) VALUES(?, ?)", 1, "Ivanov Ivan");
        executeSql(cache, "INSERT INTO person(id, fio) VALUES(?, ?)", 2, "Petrov Petr");
        executeSql(cache, "INSERT INTO person(id, fio) VALUES(?, ?)", 3, "Sidorov Sidr");

        markSyncExecutionComplete();
    }
}
