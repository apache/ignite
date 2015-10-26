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

package org.apache.ignite.examples.migration6_7;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.gridgain.client.GridClient;
import org.gridgain.client.GridClientConfiguration;
import org.gridgain.client.GridClientData;
import org.gridgain.client.GridClientDataConfiguration;
import org.gridgain.client.GridClientException;
import org.gridgain.client.GridClientFactory;
import org.gridgain.client.GridClientPartitionAffinity;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class Migration6_7 {
    /** Partitioned. */
    public static final String PARTITIONED = "partitioned";

    /** Person cache name. */
    public static final String PERSON = "personCache";

    /** Sleep. */
    public static final long SLEEP = 60 * 60 * 1000;

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        try(
            GridClient grGainClient1 = GridClientFactory.start(gg6ClientConfiguration(0));
            GridClient grGainClient2 = GridClientFactory.start(gg6ClientConfiguration(1));
            Ignite igniteClient1 = Ignition.start(igniteClientConfiguration(1));
            Ignite igniteClient2 = Ignition.start(igniteClientConfiguration(2))
        ) {
            testSimpleCache(grGainClient1, grGainClient2, igniteClient1, igniteClient2);

            testPersonCache(grGainClient1, grGainClient2, igniteClient1, igniteClient2);
        }
    }

    /**
     * @param grGainClient1 Gr gain client 1.
     * @param grGainClient2 Gr gain client 2.
     * @param igniteClient1 Ignite client 1.
     * @param igniteClient2 Ignite client 2.
     */
    private static void testSimpleCache(GridClient grGainClient1, GridClient grGainClient2, Ignite igniteClient1,
        Ignite igniteClient2) throws GridClientException {
        GridClientData grGainSimpleCache1 = grGainClient1.data(PARTITIONED);
        GridClientData grGainSimpleCache2 = grGainClient2.data(PARTITIONED);
        IgniteCache<Integer, String> igniteSimpleCache1 = igniteClient1.cache(PARTITIONED);
        IgniteCache<Integer, String> igniteSimpleCache2 = igniteClient2.cache(PARTITIONED);

        for (int i = 0; i < 100; i++) {
            grGainSimpleCache1.put(i, "str" + i);
            igniteSimpleCache1.put(i, "str" + i);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("str" + i, grGainSimpleCache2.get(i));

            assertEquals("str" + i, igniteSimpleCache2.get(i));
        }
    }

    /**
     * @param grGainClient1 Gr gain client 1.
     * @param grGainClient2 Gr gain client 2.
     * @param igniteClient1 Ignite client 1.
     * @param igniteClient2 Ignite client 2.
     */
    private static void testPersonCache(GridClient grGainClient1, GridClient grGainClient2, Ignite igniteClient1,
        Ignite igniteClient2) throws GridClientException {
        GridClientData grGainSimpleCache1 = grGainClient1.data(PERSON);
        GridClientData grGainSimpleCache2 = grGainClient2.data(PERSON);
        IgniteCache<PersonId, Person> igniteSimpleCache1 = igniteClient1.cache(PERSON);
        IgniteCache<PersonId, Person> igniteSimpleCache2 = igniteClient2.cache(PERSON);

        List<Organization> orgs = new ArrayList<Organization>() {{
            add(new Organization());
            add(new Organization());
            add(new Organization());
            add(new Organization());
        }};

        for (int i = 0; i < 100; i++) {
            Organization o = orgs.get(i % orgs.size());

            Person p = new Person(o);
            grGainSimpleCache1.put(p.id, p);
            igniteSimpleCache1.put(p.id, p);
        }

        for (Cache.Entry<PersonId, Person> e : igniteSimpleCache1) {
            assertEquals(grGainSimpleCache1.get(e.getKey()), grGainSimpleCache2.get(e.getKey()));
            assertEquals(e.getValue(), grGainSimpleCache2.get(e.getKey()));
        }

    }

    /**
     */
    private static IgniteConfiguration igniteClientConfiguration(int n) {
        return IgniteStarter.configuration().setClientMode(true).setGridName("client" + n);
    }

    /**
     * @param i I.
     */
    private static GridClientConfiguration gg6ClientConfiguration(int i) {
        GridClientConfiguration c = new GridClientConfiguration();

        c.setServers(Collections.singleton("127.0.0.1:" + (11211+i)));

        // Data 1.
        Collection<GridClientDataConfiguration> dataCfgs = new ArrayList<>();

        GridClientDataConfiguration partData = new GridClientDataConfiguration();
        partData.setName(PARTITIONED);
        partData.setAffinity(new GridClientPartitionAffinity());

        dataCfgs.add(partData);

        // Data 2.
        GridClientDataConfiguration partData2 = new GridClientDataConfiguration();
        partData2.setName(PERSON);
        partData2.setAffinity(new GridClientPartitionAffinity());

        dataCfgs.add(partData2);

        c.setDataConfigurations(dataCfgs);

        return c;
    }
}
