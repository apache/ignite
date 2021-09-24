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
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.log4j.lf5.LogLevel;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Thin client. Two ways:
 * 1 - connect -> put one value -> get one value -> disconnect (repeat)
 * 2 - connect -> putAll -> diconnect (repeat)
 */

public class ThinClientContiniusApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int singlePutClients = jNode.get("singlePutClients").asInt();
        int putAllClients = jNode.get("putAllClients").asInt();
        int justConnectClients = jNode.get("justConnectClients").asInt();
        int putAllSize = jNode.get("putAllSize").asInt();
        int putSize = jNode.get("putSize").asInt() * 1024;
        int workingTime = jNode.get("workingTime").asInt();

        markInitialized();

//        client.close();

        for(int i = 0; i < 100; i++){
            ClientConfiguration cfg = IgnitionEx.loadSpringBean(cfgPath, "thin.client.cfg");
            new oneThinClient(cfg).call();
        }
        markFinished();
    }
}

class oneThinClient implements Runnable{
    ClientConfiguration cfg;


    oneThinClient(ClientConfiguration cfg){
        this.cfg = cfg;
    }

    @Override
    public void run() {
        Logger log = LogManager.getLogManager().getLogger(this.getClass().getName());
        long connectStart = 0;
        long connectTime = 0;
        long putDataStart = 0;
        long putDataTime = 0;
        cfg.setPartitionAwarenessEnabled(true);
        connectStart = System.currentTimeMillis();
        try (IgniteClient client = Ignition.startClient(cfg)) {
            connectTime = System.currentTimeMillis() - connectStart;
            ClientCache<UUID, byte[]> cache = client.getOrCreateCache("testCache");
            putDataStart = System.currentTimeMillis();
            long putDataEnd = putDataStart + 10_000;
            while(System.currentTimeMillis() < putDataEnd){
                UUID uuid = UUID.randomUUID();
                byte[] data = new byte[10*1024];
                cache.put(uuid, data);
            }
            putDataTime = System.currentTimeMillis() - putDataStart;
            log.info("Connect time: "+connectTime+" PutTime: " +putDataTime);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
