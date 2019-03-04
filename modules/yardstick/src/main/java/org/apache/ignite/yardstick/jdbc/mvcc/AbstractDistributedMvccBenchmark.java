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

package org.apache.ignite.yardstick.jdbc.mvcc;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.jdbc.JdbcUtils.fillData;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Base for mvcc benchmarks that are running on multiply hosts.
 */
public abstract class AbstractDistributedMvccBenchmark extends IgniteAbstractBenchmark {
    /** Sql query to create load. */
    public static final String UPDATE_QRY = "UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?";

    /** Timeout in minutest for test data to be loaded. */
    public static final long DATA_WAIT_TIMEOUT_MIN = 20;

    /** Member id of the host driver is running */
    protected int memberId;
    /**
     * Number of nodes handled by driver.
     */
    protected int driversNodesCnt;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        memberId = cfg.memberId();

        if (memberId < 0)
            throw new IllegalStateException("Member id should be initialized with non-negative value");

        // We assume there is no client nodes in the cluster except clients that are yardstick drivers.
        driversNodesCnt = ignite().cluster().forClients().nodes().size();

        IgniteCountDownLatch dataIsReady = ignite().countDownLatch("fillDataLatch", 1, true, true);

        try {
            if (memberId == 0) {
                fillData(cfg, (IgniteEx)ignite(), args.range(), args.atomicMode());

                dataIsReady.countDown();
            }
            else {
                println(cfg, "No need to upload data for memberId=" + memberId + ". Just waiting");

                dataIsReady.await(DATA_WAIT_TIMEOUT_MIN, TimeUnit.MINUTES);

                println(cfg, "Data is ready.");
            }

        }
        catch (Throwable th) {
            dataIsReady.countDownAll();

            throw new RuntimeException("Fill Data failed.", th);
        }

        // Workaround for "Table TEST_LONG not found" on sql update.
        execute(new SqlFieldsQuery("SELECT COUNT(*) FROM test_long"));
    }

    /**
     * Execute specified query using started driver node.
     * Returns result using {@link QueryCursor#getAll()}.
     *
     * @param qry sql query to execute.
     */
    protected List<List<?>> execute(SqlFieldsQuery qry) {
        return ((IgniteEx)ignite())
            .context()
            .query()
            .querySqlFields(qry, false)
            .getAll();
    }
}
