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

package org.apache.ignite.examples.stockengine.approach2;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.Side;
import org.apache.ignite.examples.stockengine.domain.State;
import org.apache.ignite.examples.stockengine.domain.User;
import org.apache.ignite.internal.IgniteEx;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class RunClient2 {
    /**
     * @param args Args.
     */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/src/main/resources/ignite_replication_client.xml")) {
            // Activate cluster
            ignite.cluster().active(true);

            ignite.cluster().setBaselineTopology(((IgniteEx)ignite).context().discovery().topologyVersion());

            // Deploy services only on server nodes.
            IgniteServices svcs = ignite.services(ignite.cluster().forServers());

            // Deploy singleton.
            svcs.deployKeyAffinitySingleton("engine", new DurableEngine(), "users", 1);

            System.err.println(">> SERVICE DEPLOYED!");

            IgniteCache<Integer, User> users = ignite.cache("users");

            users.put(0, new User(0, "Alice", "Ru"));

            System.err.println(">> USER WAS CREATED");

            IgniteCache<OrderKey, Order> order = ignite.cache("orders");

            Engine engine = svcs.serviceProxy("engine", Engine.class, false);

            System.err.println(">> START ISSUING ORDERS");

            for (int i = 0;; i++) {
                order.put(
                        new OrderKey(i, 0),
                        new Order(i,
                                0,
                                Side.BUY,
                                Instrument.EUR_USD,
                        System.currentTimeMillis() + 10_000,
                                1.050 + ThreadLocalRandom.current().nextDouble(0.1),
                                100,
                                State.ACTIVE));

                System.err.println(">> CURR ORDER ID=" + i + ", TOTAL ACTIVE ORDER COUNT=" + engine.orderInWork());

                Thread.sleep(500);
            }
        }
    }
}
