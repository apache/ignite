package org.apache.ignite.internal.client.thin;

import static java.util.stream.IntStream.range;
import org.junit.Test;

/**
 * Tests for duplication in channels' list.
 */
public class ReliableChannelDuplicationTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /**
     * Test after single node cluster restart remains single channel.
     */
    @Test
    public void testSingleNodeDuplicationOnClusterRestart() throws Exception {
        startGrids(1);
        initClient(getClientConfiguration(range(0, 1).toArray()), range(0, 1).toArray());

        assertEquals(1, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());

        stopAllGrids();

        startGrids(1);

        assertEquals(1, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());
    }

    /**
     * Test after cluster restart the number of channels remains equal to the number of nodes.
     */
    @Test
    public void testMultiplyNodeDuplicationOnClusterRestart() throws Exception {
        startGrids(3);
        initClient(getClientConfiguration(range(0, 3).toArray()), range(0, 3).toArray());

        assertEquals(3, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());

        stopAllGrids();

        startGrids(3);

        assertEquals(3, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());
    }
}
