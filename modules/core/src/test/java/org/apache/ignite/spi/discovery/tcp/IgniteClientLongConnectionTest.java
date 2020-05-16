package org.apache.ignite.spi.discovery.tcp;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Checking message in the logs when the connection with the client takes a long time. */
public class IgniteClientLongConnectionTest extends GridCommonAbstractTest {

    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super
            .getConfiguration(gridName)
            .setCommunicationSpi(new TcpCommunicationSpi() {
                @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
                    try {
                        //Increasing the working time of the method so that a warning message appears in the logs.
                        TimeUnit.MILLISECONDS.sleep(CONNECTION_ESTABLISH_THRESHOLD_MS + 1);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    return super.createTcpClient(node, connIdx);
                }
            })
            .setGridLogger(listeningLog);
    }

    @Test
    public void longClientConnectionMessageTest() throws Exception {
        startGrid("serverTest");

        LogListener longClientConnLsnr = LogListener
            .matches("TCP client creation took longer than expected")
            .build();

        listeningLog.registerListener(longClientConnLsnr);

        startClientGrid("clientTest");

        assertTrue(longClientConnLsnr.check());
    }
}
