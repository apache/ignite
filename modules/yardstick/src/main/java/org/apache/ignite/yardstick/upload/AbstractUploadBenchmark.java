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

    /** Number of inserts in batch */
    public int BATCH_SIZE;

    /** {@inheritDoc} */
    public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        INSERT_SIZE = args.range();
        BATCH_SIZE = args.sqlRange();

        warmup();
    }

    /** Method to warm up Benchmark server */
    public abstract void warmup() throws Exception;

    /** create empty table */
    protected void setupData(){
        SqlFieldsQuery qry = new SqlFieldsQuery("CREATE TABLE test_long (id LONG PRIMARY KEY, val LONG);");
        GridQueryProcessor qProc = ((IgniteEx)ignite()).context().query();
        qProc.querySqlFields(qry, false);

    }

    /** {@inheritDoc} */
    public void tearDown() throws Exception {
        try(PreparedStatement count = conn.get().prepareStatement("SELECT COUNT(id) FROM test_long;")){
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
