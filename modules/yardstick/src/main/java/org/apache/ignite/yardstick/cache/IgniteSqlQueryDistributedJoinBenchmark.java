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

package org.apache.ignite.yardstick.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.model.Organization;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class IgniteSqlQueryDistributedJoinBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private int range;

    /** */
    private boolean broadcast;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        broadcast = broadcastJoin();

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        range = args.range();

        if (range <= 0)
            throw new IllegalArgumentException();

        try (IgniteDataStreamer<Object, Object> dataLdr = ignite().dataStreamer(cache.getName())) {
            for (int orgId = 0; orgId < range; orgId++) {
                dataLdr.addData(orgId, new Organization(orgId, "org" + orgId));

                int personId = range + orgId;

                Person p = new Person(personId,
                    orgId,
                    "firstName" + personId,
                    "lastName" + personId, 1000);

                dataLdr.addData(personId, p);

                if (orgId % 1000 == 0 && Thread.currentThread().isInterrupted())
                    return;
            }

            dataLdr.close();
        }

        println(cfg, "Finished populating join query [orgCnt=" + range +
            ", personCnt=" + range +
            ", broadcastJoin=" + broadcast +
            ", time=" + ((System.nanoTime() - start) / 1_000_000) + "ms]");

        executeQueryJoin(0, broadcast, true);
    }

    /**
     * @return Broadcast join flag.
     */
    protected boolean broadcastJoin() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int orgId = nextRandom(range);

        Collection<List<?>> res = executeQueryJoin(orgId, broadcast, false);

        int persons = 1;

        if (res.size() != persons)
            throw new Exception("Invalid join result [orgId=" + orgId + ", resSize=" + res.size() + ']');

        for (List<?> l : res) {
            int orgId0 = (Integer)l.get(1);

            if (orgId != orgId0)
                throw new Exception("Invalid join result [orgId=" + orgId + ", res=" + l + ']');
        }

        return true;
    }

    /**
     * @param orgId Organization ID.
     * @param broadcast Broadcast join flag.
     * @param planOnly If {@code true} just prints query plan.
     * @return Query results.
     * @throws Exception If failed.
     */
    private Collection<List<?>> executeQueryJoin(int orgId, boolean broadcast, boolean planOnly) throws Exception {
        SqlFieldsQuery qry;

        String sql;

        if (broadcast) {
            sql = "select p.id, p.orgId, p.firstName, p.lastName, o.name " +
                "from Person p " +
                "join Organization o " +
                "on p.orgId = o.id " +
                "where o.id=?";
        }
        else {
            sql = "select p.id, p.orgId, p.firstName, p.lastName, o.name " +
                "from Organization o " +
                "join Person p " +
                "on p.orgId = o._key " +
                "where o._key=?";
        }

        qry = new SqlFieldsQuery(planOnly ? ("explain " + sql) : sql);
        qry.setDistributedJoins(true);
        qry.setArgs(orgId);

        if (planOnly) {
            String plan = (String)cache.query(qry).getAll().get(0).get(0);

            println("Query execution plan:\n" + plan);

            if (broadcast) {
                if (plan.contains("batched:unicast") || !plan.contains("batched:broadcast"))
                    throw new Exception("Unexpected query plan: " + plan);
            }
            else if (!plan.contains("batched:unicast") || plan.contains("batched:broadcast"))
                throw new Exception("Unexpected query plan: " + plan);

            return null;
        }
        else
            return cache.query(qry).getAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query");
    }
}
