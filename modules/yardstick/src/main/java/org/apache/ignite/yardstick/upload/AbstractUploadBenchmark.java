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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.yardstick.jdbc.AbstractJdbcBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

public abstract class AbstractUploadBenchmark extends AbstractJdbcBenchmark {
    /** Total inserts size */
    public int INSERT_SIZE;

    /**
     * Factory that hides all test data details:
     * what query to use to create table
     * or what random arguments to set in prepared statement
     */
    QueryFactory queries = new QueryFactory();

    /** {@inheritDoc} */
    public final void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        INSERT_SIZE = args.range();

        init();
        warmup();
    }

    /** Method to init benchmark fields */
    protected void init() {
        // No-op
    }

    /**
     * Method to warm up Benchmark server
     * In upload benchmarks we need warmup action
     * and real test action to be separated
     */
    protected abstract void warmup() throws Exception;

    /** create empty table */
    protected void setupData(){
        SqlFieldsQuery qry = new SqlFieldsQuery(queries.createTable());
        GridQueryProcessor qProc = ((IgniteEx)ignite()).context().query();
        qProc.querySqlFields(qry, false);

    }

    /** {@inheritDoc} */
    public void tearDown() throws Exception {
        try(PreparedStatement count = conn.get().prepareStatement(queries.count())){
            try (ResultSet rs = count.executeQuery()) {
                rs.next();
                long size = rs.getLong(1);

                BenchmarkUtils.println(cfg, "Test table `test_long' contains " + size + " rows");
                // todo assert size == threads * sqlRange
            }
        } catch (Exception ex){
            BenchmarkUtils.println(cfg, "Exception thrown at tear down: " + ex);
        }

        super.tearDown();
    }
}
