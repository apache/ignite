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
import java.util.UUID;
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
    private static final int THREADS = 10;

    /** */
    private static final int START_TIME_WAIT_SEC = 20;

    /** */
    private static final int PRELOAD_TIME_SEC = 20;

    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws Exception {
        createTable();

        final ForkJoinPool executor = new ForkJoinPool(THREADS);
        final CountDownLatch initLatch = new CountDownLatch(THREADS);
        final AtomicLong counter = new AtomicLong();
        final AtomicBoolean preloaded = new AtomicBoolean();

        log.info("TEST | Load pool parallelism=" + executor.getParallelism());

        for (int i = 0; i < THREADS; ++i) {
            executor.submit(() -> {
                final Random rnd = new Random();
                boolean init = false;

                while (active()) {
                    try (Connection conn = thinJdbcDataSource.getConnection()) {
                        PreparedStatement ps = conn.prepareStatement("INSERT INTO SCS_DM_DOCUMENTS values(?,?,?)");

                        while (active()) {
                            long id = counter.incrementAndGet();

                            ps.setLong(1, id);
                            ps.setString(2, UUID.randomUUID().toString());
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

        if (!initLatch.await(START_TIME_WAIT_SEC, TimeUnit.SECONDS)) {
            Exception th = new IllegalStateException("Failed to start loading.");

            markBroken(th);

            throw th;
        }

        log.info("TEST | Started " + THREADS + " loading threads. Preloading...");

        synchronized (this) {
            wait(PRELOAD_TIME_SEC * 1000);
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
    private void createTable() throws Exception {
        try (Connection conn = thinJdbcDataSource.getConnection()) {
            conn.createStatement().execute("CREATE TABLE SCS_DM_DOCUMENTS(" +
                "id INT, strVal VARCHAR, decVal DECIMAL, PRIMARY KEY(id)" +
                ") WITH \"cache_name=TBG_SCS_DM_DOCUMENTS\""
            );

            try {
                ResultSet rs = conn.prepareStatement("SELECT count(1) FROM SCS_DM_DOCUMENTS").executeQuery();

                rs.next();

                int cnt = rs.getInt(1);

                if (cnt == 0)
                    log.info("TEST | Created table 'SCS_DM_DOCUMENTS'.");
                else
                    throw new IllegalStateException("Unexpected empty table count: " + cnt);
            }
            catch (Exception t) {
                t = new IllegalStateException("Failed to create table SCS_DM_DOCUMENTS.", t);

                markBroken(t);

                throw t;
            }
        }

    }
}
