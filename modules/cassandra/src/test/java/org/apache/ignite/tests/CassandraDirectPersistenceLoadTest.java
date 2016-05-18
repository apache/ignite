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

package org.apache.ignite.tests;

import org.apache.ignite.tests.load.LoadTestDriver;
import org.apache.ignite.tests.load.cassandra.BulkReadWorker;
import org.apache.ignite.tests.load.cassandra.BulkWriteWorker;
import org.apache.ignite.tests.load.cassandra.ReadWorker;
import org.apache.ignite.tests.load.cassandra.WriteWorker;
import org.apache.ignite.tests.utils.CacheStoreHelper;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

/**
 * Load tests for {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore} implementation of
 * {@link org.apache.ignite.cache.store.CacheStore} which allows to store Ignite cache data into Cassandra tables.
 */
public class CassandraDirectPersistenceLoadTest extends LoadTestDriver {
    /** */
    private static final Logger LOGGER = Logger.getLogger("CassandraLoadTests");

    /**
     *
     * @param args Test arguments.
     */
    public static void main(String[] args) {
        try {
            LOGGER.info("Cassandra load tests execution started");

            LoadTestDriver driver = new CassandraDirectPersistenceLoadTest();

            /**
             * Load test scripts could be executed from several machines. Current implementation can correctly,
             * handle situation when Cassandra keyspace/table was dropped - for example by the same load test
             * started a bit later on another machine. Moreover there is a warm up period for each load test.
             * Thus all the delays related to keyspaces/tables recreation actions will not affect performance metrics,
             * but it will be produced lots of "trash" output in the logs (related to correct handling of such
             * exceptional situation and keyspace/table recreation).
             *
             * Thus dropping test keyspaces at the beginning of the tests makes sense only for Unit tests,
             * but not for Load tests.
            **/

            //CassandraHelper.dropTestKeyspaces();

            driver.runTest("WRITE", WriteWorker.class, WriteWorker.LOGGER_NAME);

            driver.runTest("BULK_WRITE", BulkWriteWorker.class, BulkWriteWorker.LOGGER_NAME);

            driver.runTest("READ", ReadWorker.class, ReadWorker.LOGGER_NAME);

            driver.runTest("BULK_READ", BulkReadWorker.class, BulkReadWorker.LOGGER_NAME);

            /**
             * Load test script executed on one machine could complete earlier that the same load test executed from
             * another machine. Current implementation can correctly handle situation when Cassandra keyspace/table
             * was dropped (simply recreate it). But dropping keyspace/table during load tests execution and subsequent
             * recreation of such objects can have SIGNIFICANT EFFECT on final performance metrics.
             *
             * Thus dropping test keyspaces at the end of the tests makes sense only for Unit tests,
             * but not for Load tests.
             */

            //CassandraHelper.dropTestKeyspaces(); // REVIEW This line is commented by purpose?

            LOGGER.info("Cassandra load tests execution completed");
        }
        catch (Throwable e) {
            LOGGER.error("Cassandra load tests execution failed", e);
            throw new RuntimeException("Cassandra load tests execution failed", e);
        }
        finally {
            CassandraHelper.releaseCassandraResources();
        }
    }

    /** {@inheritDoc} */
    @Override protected Logger logger() {
        return LOGGER;
    }

    /** {@inheritDoc} */
    @Override protected Object setup(String logName) {
        return CacheStoreHelper.createCacheStore(
            TestsHelper.getLoadTestsCacheName(),
            TestsHelper.getLoadTestsPersistenceSettings(),
            CassandraHelper.getAdminDataSrc(),
            Logger.getLogger(logName));
    }

}
