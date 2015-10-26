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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.gridgain.client.GridClient;
import org.gridgain.client.GridClientConfiguration;
import org.gridgain.client.GridClientData;
import org.gridgain.client.GridClientDataConfiguration;
import org.gridgain.client.GridClientFactory;
import org.gridgain.client.GridClientPartitionAffinity;

/**
 *
 */
public class Migration6_7 {
    /** Partitioned. */
    public static final String PARTITIONED = "partitioned";

    /** Sleep. */
    public static final long SLEEP = 10 * 60 * 1000; // 10 min.

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        try(
            GridClient ggClient = GridClientFactory.start(gg6ClientConfiguration());
            Ignite ignite = Ignition.start(igniteClientConfiguration())
        ) {
            GridClientData data = ggClient.data(PARTITIONED);

            for (int i = 0; i < 100; i++)
                System.out.println(">>>>> Gg6 client got [i="+i+", val="+data.get(i)+"]");

            IgniteCache<Integer, String> cache = ignite.cache(PARTITIONED);

            for (int i = 0; i < 100; i++)
                System.out.println(">>>>> Ignite client got [i="+i+", val="+cache.get(i)+"]");
        }
    }

    /**
     */
    private static IgniteConfiguration igniteClientConfiguration() {
        return IgniteStarter.configuration().setClientMode(true);
    }

    /**
     */
    private static GridClientConfiguration gg6ClientConfiguration() {
        GridClientConfiguration c = new GridClientConfiguration();

        c.setServers(Collections.singleton("127.0.0.1:11211"));

        Collection<GridClientDataConfiguration> dataCfgs = new ArrayList<>();

        GridClientDataConfiguration partData = new GridClientDataConfiguration();
        partData.setName(PARTITIONED);
        partData.setAffinity(new GridClientPartitionAffinity());

        dataCfgs.add(partData);

        c.setDataConfigurations(dataCfgs);

        return c;
    }
}
