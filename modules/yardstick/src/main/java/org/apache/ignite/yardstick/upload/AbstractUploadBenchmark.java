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

package org.apache.ignite.yardstick.upload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.ignite.yardstick.jdbc.AbstractJdbcBenchmark;
import org.apache.ignite.yardstick.upload.model.QueryFactory;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Base class for upload benchmarks.
 * Designed to run test method one single time.
 * Introduces custom warmup operation.
 */
public abstract class AbstractUploadBenchmark extends AbstractJdbcBenchmark {
    /** Total inserts size. */
    long insertRowsCnt;

    /** Rows count to be inserted and deleted during warmup */
    long warmupRowsCnt;

    /** Factory that hides all the test data details. */
    protected QueryFactory queries = new QueryFactory();

    /** {@inheritDoc} */
    @Override public final void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        insertRowsCnt = args.upload.uploadRowsCnt();
        warmupRowsCnt = args.upload.warmupRowsCnt();

        init();

        // Perform warmup keeping in mind wal optimization.
        BenchmarkUtils.println(this.cfg, "Starting custom warmup");

        if (args.upload.disableWal())
            executeUpdate(QueryFactory.TURN_OFF_WAL);

        try (Connection warmupConn = uploadConnection()) {
            warmup(warmupConn);
        }

        if (args.upload.disableWal())
            executeUpdate(QueryFactory.TURN_ON_WAL);

        BenchmarkUtils.println(this.cfg, "Custom warmup finished");

        dropAndCreate();
    }

    /**
     * Inits benchmark fields.
     */
    protected void init() {
        // No-op.
    }

    /**
     * Method to warm up Benchmark server. <br/>
     * In upload benchmarks we need warmup action
     * and real test action to be separated.
     */
    protected abstract void warmup(Connection warmupConn) throws Exception;

    /**
     * Creates empty table.
     */
    @Override protected void setupData() throws Exception{
        dropAndCreate();
    }

    /**
     * Uploads data using this special connection, that may have additional
     * url parameters, such as {@code streaming=true}.
     */
    protected abstract void upload(Connection uploadConn) throws Exception;

    /** {@inheritDoc} */
    @Override public final boolean test(Map<Object, Object> ctx) throws Exception {
        if (args.upload.disableWal())
            executeUpdate(QueryFactory.TURN_OFF_WAL);

        try (Connection uploadConn = uploadConnection()) {
            upload(uploadConn);
        }

        if (args.upload.disableWal())
            executeUpdate(QueryFactory.TURN_ON_WAL);

        return true;
    }

    /**
     * Drops and re-creates test table.
     */
    private final void dropAndCreate() throws SQLException {
        executeUpdate(QueryFactory.DROP_TABLE_IF_EXISTS);
        executeUpdate(queries.createTable());
    }

    /**
     * Retrieves records count in the test table.
     */
    public long count() throws SQLException {
        try(PreparedStatement cnt = conn.get().prepareStatement(QueryFactory.COUNT)){
            try (ResultSet rs = cnt.executeQuery()) {
                rs.next();

                return rs.getLong(1);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        BenchmarkUtils.println(cfg, "Tearing down");

        try {
            long cnt = count();

            if (cnt != insertRowsCnt) {
                String msg = "Rows count is incorrect: [actual=" + cnt + ", expected=" + insertRowsCnt + "]";

                BenchmarkUtils.println(cfg, "TearDown: " + msg);

                throw new RuntimeException(msg);
            }

            BenchmarkUtils.println(cfg, "Test table contains " + cnt + " rows.");
        }
        finally {
            super.tearDown();
        }

        BenchmarkUtils.println(cfg, "TearDown successfully finished.");
    }

    /**
     * Facility method for executing update queries.
     */
    private int executeUpdate(String updQry) throws SQLException {
        try(PreparedStatement update = conn.get().prepareStatement(updQry)){
            return update.executeUpdate();
        }
    }

    /**
     *  Creates new connection only for upload purpose.
     *  This connection is special, since it may have additional jdbc url parameters.
     */
    private Connection uploadConnection() throws SQLException {
        String urlParams = "";

        // We can't just pass entire params string, due to yardstick, which relies on bash,
        // has some troubles with escaping ampersand character.
        List<String> rawParams = args.upload.uploadJdbcParams();

        if (!rawParams.isEmpty()) {
            String kvList = String.join("&", rawParams);
            urlParams = "?" + kvList;
        }

        return connection(url + urlParams);
    }
}
