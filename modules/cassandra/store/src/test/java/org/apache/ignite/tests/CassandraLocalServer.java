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
