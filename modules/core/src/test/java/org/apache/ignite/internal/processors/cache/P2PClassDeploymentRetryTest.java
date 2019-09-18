package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.lang.reflect.Constructor;
import java.net.URL;

/**
 */
public class P2PClassDeploymentRetryTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_CLS = "org.apache.ignite.tests.p2p.compute.ExternalCallable";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setNetworkTimeout(1_000)
            .setPeerClassLoadingEnabled(true)
            .setCommunicationSpi(new TestDelayedCommunicationSpi());
    }

    /**
     * @throws Exception if failed.
     */
    public void testDeploymentFromSecondAndThird() throws Exception {
        try {
            IgniteEx ignite0 = startGrid(0);
            IgniteEx ignite1 = startGrid(1);

            runJob(ignite1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    private void runJob(Ignite ignite) throws Exception {
        ClassLoader testClassLoader = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

        Constructor ctor = testClassLoader.loadClass(RUN_CLS).getConstructor();

        ignite.compute(ignite.cluster().forRemotes()).broadcast((IgniteCallable<?>)ctor.newInstance());
    }

    private class TestDelayedCommunicationSpi extends TcpCommunicationSpi {

        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {

            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDeploymentRequest) {
                GridDeploymentRequest gridDepReq = (GridDeploymentRequest)((GridIoMessage)msg).message();

                String resName = GridTestUtils.getFieldValue(gridDepReq, gridDepReq.getClass(), "rsrcName");

                assertEquals(resName.substring(0, resName.length() - ".class".length()).replace('/', '.'), RUN_CLS);

                GridTestUtils.runAsync(new Runnable() {
                    @Override public void run() {
                        log.info("Message capture on send: " + gridDepReq);

                        long netTimeout = ignite.configuration().getNetworkTimeout();
                        int tryNum = GridDeploymentInfo.DFLT_TRY_GET_RESPONSE_NUMBER;

                        //If wait less than DFLT_TRY_GET_RESPONSE_NUMBER * netTimeout,
                        //should get response correctly
                        doSleep((tryNum - 1)*netTimeout);

                        TestDelayedCommunicationSpi.super.sendMessage(node, msg, ackClosure);

                        log.info("Message was sent: " + gridDepReq);
                    }
                });

                return;
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }

}
