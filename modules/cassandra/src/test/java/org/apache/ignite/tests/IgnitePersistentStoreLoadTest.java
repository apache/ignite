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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.tests.load.LoadTestDriver;
import org.apache.ignite.tests.load.ignite.BulkReadWorker;
import org.apache.ignite.tests.load.ignite.BulkWriteWorker;
import org.apache.ignite.tests.load.ignite.ReadWorker;
import org.apache.ignite.tests.load.ignite.WriteWorker;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

/**
 * Load tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class IgnitePersistentStoreLoadTest extends LoadTestDriver {
    private static final Logger LOGGER = Logger.getLogger("IgniteLoadTests");

    public static void main(String[] args) {
        try {
            LOGGER.info("Ignite load tests execution started");

            LoadTestDriver driver = new IgnitePersistentStoreLoadTest();

            CassandraHelper.dropTestKeyspaces();

            driver.runTest("WRITE", WriteWorker.class, WriteWorker.LOGGER_NAME);

            driver.runTest("READ", ReadWorker.class, ReadWorker.LOGGER_NAME);

            driver.runTest("BULK_READ", BulkReadWorker.class, BulkReadWorker.LOGGER_NAME);

            CassandraHelper.dropTestKeyspaces();

            driver.runTest("BULK_WRITE", BulkWriteWorker.class, BulkWriteWorker.LOGGER_NAME);

            CassandraHelper.dropTestKeyspaces();

            LOGGER.info("Ignite load tests execution completed");
        }
        finally {
            CassandraHelper.releaseCassandraResources();
        }
    }

    @Override protected Logger logger() {
        return LOGGER;
    }

    @Override protected Object setup(String loggerName) {
        return Ignition.start(TestsHelper.getLoadTestsIgniteConfig());
    }

    @Override protected void tearDown(Object obj) {
        Ignite ignite = (Ignite)obj;

        if (ignite != null)
            ignite.close();
    }

}
