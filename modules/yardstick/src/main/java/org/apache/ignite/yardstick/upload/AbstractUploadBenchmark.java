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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.ignite.yardstick.jdbc.AbstractJdbcBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

public abstract class AbstractUploadBenchmark extends AbstractJdbcBenchmark {
    /** Total inserts size */
    public int INSERT_SIZE;

    /** Rows count to be inserted and deleted during warmup */
    public static final int WARMUP_ROWS_CNT = 3000_000;

    /**
     * Factory that hides all test data details:
     * what query to use to create table
     * or what random arguments to set in prepared statement
     */
    QueryFactory queries = new QueryFactory();

    /** {@inheritDoc} */
    @Override public final void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        INSERT_SIZE = args.range();

        init();
        warmup0();
        dropAndCreate();
    }

    /** Method to init benchmark fields */
    protected void init() {
        // No-op
    }

    /** Perform warmup keeping in mind wal optimization */
    private void warmup0() throws Exception{
        BenchmarkUtils.println(cfg, "Starting custom warmup");

        if (args.switchWal())
            executeUpdate(queries.turnOffWal());

        warmup();

        if (args.switchWal())
            executeUpdate(queries.turnOnWal());

        BenchmarkUtils.println(cfg, "Custom warmup finished");

    }

    /**
     * Method to warm up Benchmark server
     * In upload benchmarks we need warmup action
     * and real test action to be separated
     */
    protected abstract void warmup() throws Exception;

    /** create empty table */
    @Override protected void setupData() throws Exception{
        dropAndCreate();
    }

    protected abstract void upload() throws Exception;

    /** {@inheritDoc} */
    @Override public final boolean test(Map<Object, Object> ctx) throws Exception {
        if (args.switchWal())
            executeUpdate(queries.turnOffWal());

        upload();

        if (args.switchWal())
            executeUpdate(queries.turnOnWal());

        return true;
    }

    /** Drops and re-creates test table */
    private void dropAndCreate() throws SQLException{
        try (PreparedStatement drop = conn.get().prepareStatement(queries.dropTableIfExists())) {
            drop.executeUpdate();
        }

        try (PreparedStatement create = conn.get().prepareStatement(queries.createTable())) {
            create.executeUpdate();
        }
    }

    /** {@inheritDoc} */
    public void tearDown() throws Exception {
        try(PreparedStatement count = conn.get().prepareStatement(queries.count())){
            try (ResultSet rs = count.executeQuery()) {
                rs.next();
                long size = rs.getLong(1);

                BenchmarkUtils.println(cfg, "Test table contains " + size + " rows");
                // todo assert size == threads * sqlRange
            }
        } catch (Exception ex){
            BenchmarkUtils.println(cfg, "Exception thrown at tear down: " + ex);
        } finally {
            super.tearDown();
        }
    }


    /** Facility method for executing update queries */
    private int executeUpdate(String updQry) throws SQLException{
        try(PreparedStatement update = conn.get().prepareStatement(updQry)){
            return update.executeUpdate();
        }
    }
}
