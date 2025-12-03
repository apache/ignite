package org.apache.ignite.util;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Test for checkpoint in control.sh command. */
public class GridCommandHandlerCheckpointTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
        injectTestSystemOut();
        super.beforeTest();
    }

    /** */
    @Test
    public void testCheckpoint() throws Exception {
        IgniteEx srv = startGrids(2);
        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "test_reason"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "10000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "planned", "--wait-for-finish", "--timeout", "5000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));
    }
}