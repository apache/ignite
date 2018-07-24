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

package org.apache.ignite.tensorflow.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Prerequisites: be aware that to successfully run this example you need to have Python and TensorFlow installed on
 * your machine. To find out how to install Python please take a look this page: https://www.python.org/downloads/. To
 * install TensorFlow see this web site: https://www.tensorflow.org/install/.
 *
 * Example that shows how to use {@link TensorFlowClusterGatewayManager} and start, maintain and stop TensorFlow
 * cluster.
 */
public class TensorFlowClusterExample {
    /** Run example. */
    public static void main(String... args) throws InterruptedException {
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setClientMode(false);

        try (Ignite ignite = Ignition.start(configuration)) {
            System.out.println(">>> TensorFlow cluster example started.");

            CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));
            cacheConfiguration.setName("TEST_CACHE");

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfiguration);
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            System.out.println(">>> Cache created.");
        }
    }
}
