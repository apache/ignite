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

package org.apache.ignite.examples.client;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.ThinClientKubernetesAddressFinder;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;

/**
 * Demonstrates how to use Ignite thin client within the Kubernetes cluster using KubernetesConnectionConfiguration.
 * <p>
 * Prerequisites:
 * <ul>
 * <li>Running Ignite Kubernetes cluster. Check modules/kubernetes/DEVNOTES.md as an example of how to run a cluster locally.</li>
 * <li>A thin client application should be run within the cluster to have access to Ignite nodes pods.</li>
 * <li>KubernetesConnectionConfiguration must be in sync with the Ignite nodes configuration.</li>
 * </ul>
 * </p>
 */
public class ClientKubernetesPutGetExample {
    /**
     * Entry point.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        KubernetesConnectionConfiguration kcfg = new KubernetesConnectionConfiguration();
        kcfg.setNamespace("ignite");

        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddressesFinder(new ThinClientKubernetesAddressFinder(kcfg));

        try (IgniteClient igniteClient = Ignition.startClient(cfg)) {
            System.out.println();
            System.out.println(">>> Thin client put-get example started.");

            final String CACHE_NAME = "put-get-example";

            ClientCache<Integer, Address> cache = igniteClient.getOrCreateCache(CACHE_NAME);

            System.out.format(">>> Created cache [%s].\n", CACHE_NAME);

            Integer key = 1;
            Address val = new Address("1545 Jackson Street", 94612);

            cache.put(key, val);

            System.out.format(">>> Saved [%s] in the cache.\n", val);

            Address cachedVal = cache.get(key);

            System.out.format(">>> Loaded [%s] from the cache.\n", cachedVal);
        }
    }
}
