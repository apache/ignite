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

package org.apache.ignite.sqltests;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.platform.model.Department;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.result.ResultInterface;
import org.junit.Test;

/**
 * H2 engine adds extra column in results set.
 * Test check that this scenario will be handled correctly.
 * @see ResultInterface#currentRow()
 * @see ResultInterface#getVisibleColumnCount()
 */
public class ExtraColumnInH2RowsTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testExtraColumnIgnored() throws Exception {
        try (IgniteEx ign = startGrid()) {
            int parts = 32;

            IgniteCache<Integer, Department> cache = ign.createCache(new CacheConfiguration<Integer, Department>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName("T")
                    .setFields(new LinkedHashMap<>(Map.of("id", Integer.class.getName(), "name", String.class.getName())))
                    .setKeyType(Integer.class.getName())
                    .setValueType(Department.class.getName())))
                .setAffinity(new RendezvousAffinityFunction(false, parts)));

            Map<Integer, IgniteBiTuple<Integer, Department>> data = new HashMap<>();

            for (int part = 0; part < parts; part++) {
                Integer key = partitionKeys(cache, part, 1, 0).get(0);
                Department val = new Department(String.valueOf(part));

                cache.put(key, val);
                data.put(part, F.t(key, val));
            }

            try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                check(parts, qry -> cli.cache(DEFAULT_CACHE_NAME).query(qry).getAll(), data);
            }

            check(parts, qry -> ign.cache(DEFAULT_CACHE_NAME).query(qry).getAll(), data);

            try (IgniteEx cli = startClientGrid("client")) {
                check(parts, qry -> cli.cache(DEFAULT_CACHE_NAME).query(qry).getAll(), data);
            }
        }
    }

    /** */
    private static void check(
        int parts,
        Function<SqlFieldsQuery, List<List<?>>> qryExec,
        Map<Integer, IgniteBiTuple<Integer, Department>> data
    ) {
        data = new HashMap<>(data);

        for (int part = 0; part < parts; part++) {
            List<List<?>> res = qryExec.apply(new SqlFieldsQuery("select _key, _val from T order by id asc").setPartitions(part));

            assertEquals(1, res.size());
            assertEquals(2, res.get(0).size());

            IgniteBiTuple<Integer, Department> t = data.remove(part);

            assertEquals(t.get1(), res.get(0).get(0));
            assertEquals(t.get2().getName(), ((Department)res.get(0).get(1)).getName());

            // Check empty result set fetched OK.
            qryExec.apply(new SqlFieldsQuery("select _key, _val from T WHERE name IS NULL order by id asc").setPartitions(part));
        }

        assertTrue(data.isEmpty());
    }
}
