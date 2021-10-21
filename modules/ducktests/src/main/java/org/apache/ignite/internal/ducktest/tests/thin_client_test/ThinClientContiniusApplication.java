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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

/** Run multiple Thin Clients making some work for a given time */
public class ThinClientContiniusApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override
    protected void run(JsonNode jsonNode) throws Exception {
        int connectClients = jsonNode.get("connectClients").asInt();

        int putClients = jsonNode.get("putClients").asInt();

        int putAllClients = jsonNode.get("putAllClients").asInt();

        int runTime = jsonNode.get("runTime").asInt();

        client.close();

        markInitialized();

        log.info("RUN CLIENTS");

        ClientConfiguration cfg = IgnitionEx.loadSpringBean(cfgPath, "thin.client.cfg");

        List<List<Long>> connectTimes = new ArrayList<>();

        startClients(ClientType.CONNECT, connectClients, connectTimes, cfg);

        startClients(ClientType.PUT, putClients, connectTimes, cfg);

        startClients(ClientType.PUTALL, putAllClients, connectTimes, cfg);

        log.info("START WAITING");

        TimeUnit.SECONDS.sleep(runTime);

        log.info("STOP WAITING");

        connectTimes.forEach(log::info);

        markFinished();
    }

    /** Internal function to start clients */
    private void startClients(ClientType type, int count, List<List<Long>> times, ClientConfiguration cfg){
        for (int i = 0; i < count; i++) {
            List<Long> connectTime = new ArrayList<>();

            times.add(connectTime);

            new Thread(new ThinClientContiniusRunner(cfg, connectTime, type)).start();
        }
    }
}

