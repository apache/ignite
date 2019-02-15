/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    /** */
    private static final Logger LOGGER = Logger.getLogger("IgniteLoadTests");

    /**
     * test starter.
     *
     * @param args Test arguments.
     */
    public static void main(String[] args) {
        try {
            LOGGER.info("Ignite load tests execution started");

            LoadTestDriver driver = new IgnitePersistentStoreLoadTest();

            /**
             * Load test scripts could be executed from several machines. Current implementation can correctly,
             * handle situation when Cassandra keyspace/table was dropped - for example by the same load test
             * started a bit later on another machine. Moreover there is a warm up period for each load test.
             * Thus all the delays related to keyspaces/tables recreation actions will not affect performance metrics,
             * but it will be produced lots of "trash" output in the logs (related to correct handling of such
             * exceptional situation and keyspace/table recreation).
             *
             * Thus dropping test keyspaces makes sense only for Unit tests, but not for Load tests.
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

            //CassandraHelper.dropTestKeyspaces();

            LOGGER.info("Ignite load tests execution completed");
        }
        catch (Throwable e) {
            LOGGER.error("Ignite load tests execution failed", e);
            throw new RuntimeException("Ignite load tests execution failed", e);
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
        return Ignition.start(TestsHelper.getLoadTestsIgniteConfig());
    }

    /** {@inheritDoc} */
    @Override protected void tearDown(Object obj) {
        Ignite ignite = (Ignite)obj;

        if (ignite != null)
            ignite.close();
    }
}
