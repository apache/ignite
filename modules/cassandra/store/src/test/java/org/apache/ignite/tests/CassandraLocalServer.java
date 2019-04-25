/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests;

import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.log4j.Logger;

/**
 * Simple helper class to run Cassandra on localhost
 */
public class CassandraLocalServer {
    /** */
    private static final Logger LOGGER = Logger.getLogger(CassandraLocalServer.class.getName());

    /** */
    public static void main(String[] args) {
        try {
            CassandraHelper.startEmbeddedCassandra(LOGGER);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to start embedded Cassandra instance", e);
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();

        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();

        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();

        while (true) {
            try {
                System.out.println("Cassandra server running");

                Thread.sleep(10000);
            }
            catch (Throwable e) {
                throw new RuntimeException("Cassandra server terminated", e);
            }
        }
    }
}
