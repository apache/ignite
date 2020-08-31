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
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.examples.model.Address;

/**
 * Demonstrates how to use Ignite thin client for basic put/get cache operations.
 * <p>
 * Prerequisites:
 * <ul>
 * <li>An Ignite server node must be running on local host.</li>
 * </ul>
 * </p>
 */
public class ClientPutGetExample {
    /** Entry point. */
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");

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
        catch (ClientException e) {
            System.err.println(e.getMessage());
        }
        catch (Exception e) {
            System.err.format("Unexpected failure: %s\n", e);
        }
    }
}
