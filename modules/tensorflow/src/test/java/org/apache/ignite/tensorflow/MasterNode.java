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

package org.apache.ignite.tensorflow;

import org.apache.ignite.tensorflow.cluster.TensorFlowClusterService;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.ServiceConfiguration;

public class MasterNode {

    public static void main(String... args) throws InterruptedException {
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setClientMode(false);

        try (Ignite ignite = Ignition.start(configuration)) {
            CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));
            cacheConfiguration.setName("TEST_CACHE_1");

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfiguration);
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            String id = UUID.randomUUID().toString();
            ServiceConfiguration cfg = new ServiceConfiguration();
            cfg.setName(id);
            cfg.setService(new TensorFlowClusterService("TEST_CACHE_1"));
            cfg.setTotalCount(1);

            System.out.println("Deploy...");
            ignite.services().deploy(cfg);

            Thread.currentThread().join();
        }
    }
}
