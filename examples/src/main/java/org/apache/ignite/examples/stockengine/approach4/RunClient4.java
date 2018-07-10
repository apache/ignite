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

package org.apache.ignite.examples.stockengine.approach4;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.examples.stockengine.approach2.Engine;
import org.apache.ignite.examples.stockengine.approach2.OrderKey;
import org.apache.ignite.examples.stockengine.approach3.ReplicatedEngine;
import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.Side;
import org.apache.ignite.examples.stockengine.domain.State;
import org.apache.ignite.examples.stockengine.domain.User;
import org.apache.ignite.internal.IgniteEx;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class RunClient4 {
    public static int ALICE_ID = -1;
    public static int BOB_ID = -1;
    public static int CAROL_ID = -1;

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/src/main/resources/ignite_partitioning_client2.xml")) {
            // Activate cluster
            ignite.cluster().active(true);

            ignite.cluster().setBaselineTopology(((IgniteEx)ignite).context().discovery().topologyVersion());

            // Deploy services only on server nodes.
            IgniteServices svcs = ignite.services(ignite.cluster().forServers());

            // Deploy node singleton.
            svcs.deployNodeSingleton("engine", new ReplicatedEngine());

            chooseUserIds(ignite);

            System.err.println(">> SERVICE DEPLOYED!");

            IgniteCache<Integer, User> users = ignite.cache("users");

            users.put(ALICE_ID, new User(ALICE_ID, "Alice", "Ru"));
            users.put(BOB_ID, new User(BOB_ID, "Bob", "En"));
            users.put(CAROL_ID, new User(CAROL_ID, "Carol", "Fr"));

            System.err.println(">> USERS WERE CREATED");

            IgniteCache<OrderKey, Order> order = ignite.cache("orders");

            Engine engine = svcs.serviceProxy("engine", Engine.class, false);

            System.err.println(">> START ISSUING ORDERS");

            for (int i = 0;; i++) {
                newOrderFor(order, i, ALICE_ID, Side.BUY);

                newOrderFor(order, i, BOB_ID, Side.SELL);

                newOrderFor(order, i, CAROL_ID, Side.SELL);

                System.err.println(">> CURR ORDER ID=" + i + ", TOTAL ACTIVE ORDER COUNT=" + engine.orderInWork());

                Thread.sleep(500);
            }
        }
    }

    private static void newOrderFor(IgniteCache<OrderKey, Order> order, int orderId, int userId, Side buy) {
        order.put(
                new OrderKey(orderId, userId),
                new Order(orderId,
                        userId,
                        buy,
                        Instrument.EUR_USD,
                        System.currentTimeMillis() + 10_000,
                        1.050 + ThreadLocalRandom.current().nextDouble(0.1),
                        100,
                        State.ACTIVE));
    }


    private static void chooseUserIds(Ignite ignite) {
        Affinity<Integer> affinity = ignite.affinity("users");

        Set<ClusterNode> alreadyMapped = new HashSet<>();

        for (int j = 0; j < 1024; j++) {
            ClusterNode clusterNode = affinity.mapKeyToNode(j);

            if (alreadyMapped.add(clusterNode)) {
                if (ALICE_ID == -1)
                    ALICE_ID = j;
                else if (BOB_ID == -1)
                    BOB_ID = j;
                else {
                    CAROL_ID = j;

                    break;
                }
            }
        }
    }
}
