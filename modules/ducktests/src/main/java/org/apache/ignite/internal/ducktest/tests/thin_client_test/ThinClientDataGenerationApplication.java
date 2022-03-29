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
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import java.util.*;
import java.util.concurrent.*;

/**
 * Thin client. Cache test: put, get, remove.
 */
public class ThinClientDataGenerationApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int backups = Optional.ofNullable(jsonNode.get("backups")).map(JsonNode::asInt).orElse(1);
        String cacheName = Optional.ofNullable(jsonNode.get("cacheName")).map(JsonNode::asText).orElse("TEST-CACHE");
        int entrySize = jsonNode.get("entrySize").asInt();
        int from = jsonNode.get("from").asInt();
        int to = jsonNode.get("to").asInt();
        int preloaders = Optional.ofNullable(jsonNode.get("preloaders")).map(JsonNode::asInt).orElse(1);
        String preloadersToken = Optional.ofNullable(jsonNode.get("preloadersToken")).map(JsonNode::asText).orElse("token");
        String zkConnectionString = Optional.ofNullable(jsonNode.get("zookeeperConnectionString")).map(JsonNode::asText).orElse(null);
        int threads = Optional.ofNullable(jsonNode.get("threads")).map(JsonNode::asInt).orElse(1);
        int batchSize = Optional.ofNullable(jsonNode.get("batchSize")).map(JsonNode::asInt).orElse(50000);
        int jobSize = Optional.ofNullable(jsonNode.get("jobSize")).map(JsonNode::asInt).orElse(3);
        int timeoutSecs = Optional.ofNullable(jsonNode.get("timeoutSecs")).map(JsonNode::asInt).orElse(3600);

        markInitialized();

        if (preloaders > 1 && zkConnectionString != null) {
            RetryPolicy retryPolicy = new RetryUntilElapsed(timeoutSecs * 1000, 200);
            CuratorFramework zk = CuratorFrameworkFactory.newClient(zkConnectionString, retryPolicy);
            zk.start();
            new DistributedDoubleBarrier(zk, "/" + preloadersToken, preloaders).enter(timeoutSecs, TimeUnit.SECONDS);
        }

        client.getOrCreateCache(new ClientCacheConfiguration().setName(cacheName).setBackups(backups).setStatisticsEnabled(true));

        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        List<Future<JobResult>> results = new LinkedList<>();

        int i = from;
        while (i < to) {
            int j = i + batchSize * jobSize;
            if (j > to)
                j = to;
            results.add(executorService.submit(new PutJob(cfgPath, cacheName, i, j, batchSize, entrySize, false)));
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

//        client.destroyCache(cacheName);

        if (errors > 0)
            markBroken(new RuntimeException(String.format("%d jobs of total %d failed", errors, results.size())));
        else
            markFinished();
    }

    static class JobResult {
        int error;
        int ok;
        String errorMessage;

        private JobResult(int error, int ok, String errorMessage) {
            this.error = error;
            this.ok = ok;
            this.errorMessage = errorMessage;
        }

        public static JobResult error(String errorMessage) {
            return new JobResult(1, 0, errorMessage);
        }

        public static JobResult ok() {
            return new JobResult(0, 1, null);
        }
    }

    static class PutJob implements Callable<JobResult> {
        private final String cfgPath;
        private final String cacheName;
        private final int from;
        private final int to;
        private final int batchSize;
        private final int entrySize;
        private final boolean async;

        PutJob(String cfgPath, String cacheName, int from, int to, int batchSize, int entrySize, boolean async) {
            this.cfgPath = cfgPath;
            this.cacheName = cacheName;
            this.from = from;
            this.to = to;
            this.batchSize = batchSize;
            this.entrySize = entrySize;
            this.async = async;
        }

        @Override
        public JobResult call() {
            log.info(String.format("Start job: [%s]", toString()));

            try (IgniteClient client = getClient()) {

                ClientCache<Integer, BinaryObject> cache = client.getOrCreateCache(cacheName);

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
                if (!batch.isEmpty())
                    cache.putAll(batch);

            } catch (Throwable e) {
                log.error(String.format("Job failed: [%s]", this.toString()), e);
                return JobResult.error(e.getMessage());
            }

            return JobResult.ok();
        }

        private IgniteClient getClient() throws IgniteCheckedException {
            ClientConfiguration cfg = IgnitionEx.loadSpringBean(cfgPath, "thin.client.cfg");
            return Ignition.startClient(cfg);
        }

        @Override
        public String toString() {
            return "PutJob{" +
                    "cfgPath='" + cfgPath + '\'' +
                    ", cacheName='" + cacheName + '\'' +
                    ", from=" + from +
                    ", to=" + to +
                    ", batchSize=" + batchSize +
                    ", entrySize=" + entrySize +
                    ", async=" + async +
                    '}';
        }
    }
}
