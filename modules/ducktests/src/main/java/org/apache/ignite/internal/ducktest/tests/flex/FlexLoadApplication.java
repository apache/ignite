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

package org.apache.ignite.internal.ducktest.tests.flex;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/** */
public class FlexLoadApplication extends IgniteAwareApplication {
    /** */
    private static final int WAIT_START_SECS = 20;

    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws Exception {
        final int preloadDurSec = jsonNode.get("preloadDurSec").asInt();
        final int threads = jsonNode.get("threads").asInt();
        final String cacheName = jsonNode.get("cacheName").asText();
        final String tableName = jsonNode.get("tableName").asText();

        createTable(cacheName, tableName);

        final ForkJoinPool executor = new ForkJoinPool(threads);
        final CountDownLatch initLatch = new CountDownLatch(threads);
        final AtomicLong counter = new AtomicLong();
        final AtomicBoolean preloaded = new AtomicBoolean();

        log.info("TEST | Load pool parallelism=" + executor.getParallelism());

        for (int i = 0; i < threads; ++i) {
            executor.submit(() -> {
                final Random rnd = new Random();
                boolean init = false;

                while (active()) {
                    try (Connection conn = thinJdbcDataSource.getConnection()) {
                        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + cacheName + " values(?,?,?)");

                        while (active()) {
                            long id = counter.incrementAndGet();

                            ps.setLong(1, id);

                            byte[] data = new byte[rnd.nextInt(2048)];
                            rnd.nextBytes(data);
                            ps.setString(2, new String(data));

                            ps.setBigDecimal(3, BigDecimal.valueOf(rnd.nextDouble()));

                            int res = ps.executeUpdate();

                            if (res != 1)
                                throw new IllegalStateException("Failed to insert a row. The results is not 1.");

                            if (!init) {
                                init = true;

                                initLatch.countDown();
                            }
                        }
                    }
                    catch (Throwable th) {
                        if (!preloaded.get()) {
                            log.error("TEST | Failed to preload. Marking as broken.", th);

                            markBroken(th);

                            synchronized (this) {
                                notifyAll();
                            }
                        }
                        else
                            log.info("TEST | Failed to load. Recreating connection. Err: " + th.getMessage());
                    }
                }
            });
        }

        if (!active())
            return;

        if (!initLatch.await(WAIT_START_SECS, TimeUnit.SECONDS)) {
            Exception th = new IllegalStateException("Failed to start loading.");

            markBroken(th);

            throw th;
        }

        log.info("TEST | Started " + threads + " loading threads. Preloading...");

        synchronized (this) {
            wait(preloadDurSec * 1000);
        }

        preloaded.set(true);

        log.info("TEST | Preloaded. Loaded about " + counter.get() + " records. Continue loading...");

        markInitialized();

        while (active()) {
            synchronized (this) {
                wait(150);
            }
        }

        log.info("TEST | Stopping. Loaded about " + counter.get() + " records.");

        markFinished();
    }

    /** */
    private void createTable(String tableName, String cacheName) throws Exception {
        try (Connection conn = thinJdbcDataSource.getConnection()) {
            conn.createStatement().execute("CREATE TABLE " + tableName + "(" +
                "id INT, strVal VARCHAR, decVal DECIMAL, PRIMARY KEY(id)" +
                ") WITH \"cache_name=" + cacheName + ",atomicity=TRANSACTIONAL\""
            );

            try {
                ResultSet rs = conn.prepareStatement("SELECT count(1) FROM " + tableName).executeQuery();

                rs.next();

                int cnt = rs.getInt(1);

                if (cnt == 0)
                    log.info("TEST | Created table '" + tableName + "'.");
                else
                    throw new IllegalStateException("Unexpected empty table count: " + cnt);
            }
            catch (Exception t) {
                t = new IllegalStateException("Failed to create table " + tableName + ".", t);

                markBroken(t);

                throw t;
            }
        }
    }
}
