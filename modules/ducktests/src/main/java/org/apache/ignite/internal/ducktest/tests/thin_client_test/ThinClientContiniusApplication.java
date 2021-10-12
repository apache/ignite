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

package org.apache.ignite.internal.ducktest.tests.thin_client_test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Thin clients.
 * connectClients connect, wait, disconnect, repeat
 * putClients - connect, put many times, disconnect, repeat
 * putAllClients - connect, putAll, disconnnet, repeat
 */
enum ClientType {
    /** Just connect */
    CONNECT,
    /** Make some puts */
    PUT,
    /** Make putAll */
    PUTALL
}

/**
 * Run multiple Thin Clients making some work for a given time
 */
public class ThinClientContiniusApplication extends IgniteAwareApplication {

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int connectClients = jsonNode.get("connectClients").asInt();
        int putClients = jsonNode.get("putClients").asInt();
        int putAllClients = jsonNode.get("putAllClients").asInt();
        int runTime = jsonNode.get("runTime").asInt();

        client.close();
        markInitialized();

        log.info("RUN CLIENTS");

        ClientConfiguration cfg = IgnitionEx.loadSpringBean(cfgPath, "thin.client.cfg");
        ExecutorService executor = Executors.newFixedThreadPool(connectClients + putClients + putAllClients);
        List<List<Long>> connectTimes = new ArrayList<>();

        for (int i = 0; i < connectClients; i++) {
            List<Long> connectTime = new ArrayList<>();
            connectTimes.add(connectTime);
            executor.submit(new oneThinClient(cfg, connectTime, ClientType.CONNECT));
        }
        for (int i = 0; i < putClients; i++) {
            List<Long> connectTime = new ArrayList<>();
            connectTimes.add(connectTime);
            executor.submit(new oneThinClient(cfg, connectTime, ClientType.PUT));
        }
        for (int i = 0; i < putAllClients; i++) {
            List<Long> connectTime = new ArrayList<>();
            connectTimes.add(connectTime);
            executor.submit(new oneThinClient(cfg, connectTime, ClientType.PUTALL));
        }

        log.info("START WAITING");
        TimeUnit.SECONDS.sleep(runTime);
        log.info("STOP WAITING");
        client.close();

        connectTimes.forEach(log::info);

        markFinished();
    }
}

/**
 * Start Thin Client making some operations for a given time.
 *
 * @param cfg Ignite Thin Client COnfiguration.
 * @param connectTime external variable to save connection times
 * @param enum defining client behaviour
 */
class oneThinClient implements Runnable {

    /** Size of one entry. */
    private static final int DATA_SIZE = 15;

    /** Time of one iteration. */
    private static final int RUN_TIME = 1000;

    /** Size of Map to putAll. */
    private static final int PUT_ALL_SIZE = 1000;

    /** Ignite Client configuration. */
    ClientConfiguration cfg;

    /** Time of connection. */
    List<Long> connectTime;

    /** Type of client. */
    ClientType type;

    /** {@inheritDoc} */
    oneThinClient(ClientConfiguration cfg, List<Long> connectTime, ClientType type) {
        this.cfg = cfg;
        this.type = type;
        this.connectTime = connectTime;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        long connectStart;
        cfg.setPartitionAwarenessEnabled(true);
        while (!Thread.currentThread().isInterrupted()) {
            connectStart = System.currentTimeMillis();
            try (IgniteClient client = Ignition.startClient(cfg)) {
                connectTime.add(System.currentTimeMillis() - connectStart);
                ClientCache<UUID, byte[]> cache = client.getOrCreateCache("testCache");
                long stopTyme = System.currentTimeMillis() + RUN_TIME;
                switch (type) {
                    case CONNECT:
                        TimeUnit.MILLISECONDS.sleep(RUN_TIME);
                        break;
                    case PUT:
                        while (stopTyme > System.currentTimeMillis()) {
                            cache.put(UUID.randomUUID(), new byte[DATA_SIZE * 1024]);
                        }
                        break;
                    case PUTALL:
                        while (stopTyme > System.currentTimeMillis()) {
                            Map<UUID, byte[]> data = new HashMap<>();
                            for (int i = 0; i < PUT_ALL_SIZE; i++) {
                                data.put(UUID.randomUUID(), new byte[DATA_SIZE * 1024]);
                            }
                            cache.putAll(data);
                        }
                        break;
                    default:
                        throw new IgniteCheckedException("Unknown operation: " + type + ".");
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
