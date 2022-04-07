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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import java.util.*;
import java.util.concurrent.*;

/**
 * Application generates cache data via the thin client.
 */
public class ThinClientDataGenerationApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {

        String cacheName = Optional.ofNullable(jsonNode.get("cacheName")).map(JsonNode::asText).orElse("TEST-CACHE");
        int entrySize = jsonNode.get("entrySize").asInt();
        int from = jsonNode.get("from").asInt();
        int to = jsonNode.get("to").asInt();

        int threads = Optional.ofNullable(jsonNode.get("threads")).map(JsonNode::asInt).orElse(1);
        int batchSize = Optional.ofNullable(jsonNode.get("batchSize")).map(JsonNode::asInt).orElse(50000);
        int batchesPerTask = Optional.ofNullable(jsonNode.get("batchesPerTask")).map(JsonNode::asInt).orElse(3);
        // Whether create new Ignite Client connection for each task
        boolean clientPerTask = Optional.ofNullable(jsonNode.get("clientPerTask")).map(JsonNode::asBoolean).orElse(true);

        int timeoutSecs = Optional.ofNullable(jsonNode.get("timeoutSecs")).map(JsonNode::asInt).orElse(3600);

        markInitialized();

        if (clientPerTask)
            client.close();

        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        List<Future<JobResult>> results = new LinkedList<>();

        int i = from;
        while (i < to) {
            int j = i + batchSize * batchesPerTask;
            if (j > to)
                j = to;
            results.add(executorService.submit(new PutJob(cfgPath, cacheName, i, j, batchSize, entrySize,
                    clientPerTask ? null : client)));
            i = j;
        }

        int errors = 0;
        for (Future<JobResult> result : results) {
            errors += result.get(timeoutSecs, TimeUnit.SECONDS).error;
        }

        executorService.shutdown();
        log.info("executor shutdown, start waiting");
        if (executorService.awaitTermination(timeoutSecs, TimeUnit.SECONDS)) {
            log.info("executor terminated");
        } else {
            log.info("timeout during executor termination");
        }

        log.info(String.format("Total jobs: %d; errors: %d", results.size(), errors));

        if (errors > 0)
            markBroken(new RuntimeException(String.format("%d jobs of total %d failed", errors, results.size())));
        else
            markFinished();
    }

    static class JobResult {
        int error;
        int ok;

        private JobResult(int error, int ok) {
            this.error = error;
            this.ok = ok;
        }

        public static JobResult error() {
            return new JobResult(1, 0);
        }
        public static JobResult ok() {
            return new JobResult(0, 1);
        }
    }

    static class PutJob implements Callable<JobResult> {
        private final String cfgPath;
        private final String cacheName;
        private final int from;
        private final int to;
        private final int batchSize;
        private final int entrySize;
        private final boolean createClientConnection;
        private IgniteClient client;

        PutJob(String cfgPath, String cacheName, int from, int to, int batchSize, int entrySize, IgniteClient client) {
            this.cfgPath = cfgPath;
            this.cacheName = cacheName;
            this.from = from;
            this.to = to;
            this.batchSize = batchSize;
            this.entrySize = entrySize;
            this.createClientConnection = (client == null);
            this.client = client;
        }

        @Override
        public JobResult call() {
            log.info(String.format("Start job: [%s]", toString()));

            try {
                if (createClientConnection)
                    client = getClient();

                ClientCache<Integer, BinaryObject> cache = client.cache(cacheName);

                BinaryObjectBuilder builder = client.binary().builder("org.apache.ignite.ducktest.DataBinary");

                byte[] data = new byte[entrySize];

                ThreadLocalRandom.current().nextBytes(data);

                HashMap<Integer, BinaryObject> batch = new LinkedHashMap<>();
                for (int i = from; i < to; i++) {
                    builder.setField("key", i);
                    builder.setField("data", data);

                    batch.put(i, builder.build());
                    if (batch.size() == batchSize) {
                        cache.putAll(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    cache.putAll(batch);
                }

            } catch (Throwable e) {
                if (createClientConnection && client != null) {
                    client.close();
                }

                log.error(String.format("Failed job: [%s]", this.toString()), e);
                return JobResult.error();
            }

            if (createClientConnection && client != null)
                client.close();

            log.info(String.format("Finish job: [%s]", this.toString()));
            return JobResult.ok();
        }

        private IgniteClient getClient() throws IgniteCheckedException {
            ClientConfiguration cfg = IgnitionEx.loadSpringBean(cfgPath, "thin.client.cfg");
            return Ignition.startClient(cfg);
        }

        @Override
        public String toString() {
            return "PutJob{" +
                    "from=" + from +
                    ", to=" + to +
                    ", batchSize=" + batchSize +
                    ", entrySize=" + entrySize +
                    ", cacheName='" + cacheName + '\'' +
                    ", createClientConnection=" + createClientConnection +
                    '}';
        }
    }
}
