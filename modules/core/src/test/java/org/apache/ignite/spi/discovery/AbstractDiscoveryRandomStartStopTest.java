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

package org.apache.ignite.spi.discovery;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;

/**
 * Base discovery random start-stop test class.
 * @param <T> Discovery spi type.
 */
public abstract class AbstractDiscoveryRandomStartStopTest<T extends DiscoverySpi> extends GridSpiAbstractTest<T> {
    /** */
    private static final int DFLT_MAX_INTERVAL = 10;

    /** */
    private volatile boolean semaphore = true;

    /** */
    private boolean isRunning;

    /** */
    private Pinger pinger;

    /** */
    private class Pinger extends Thread {
        /** */
        private final Object mux = new Object();

        /** */
        private volatile boolean canceled;

        /** {@inheritDoc} */
        @SuppressWarnings({"UnusedCatchParameter"})
        @Override public void run() {
            while (!canceled) {
                try {
                    if (getSpi() != null) {
                        Collection<ClusterNode> nodes = getSpi().getRemoteNodes();

                        for (ClusterNode item : nodes) {
                            boolean flag = getSpi().pingNode(item.id());

                            if (flag) {
                                info("Ping node [nodeId=" + item.id().toString().toUpperCase() +
                                    ", pingResult=" + flag + ']');
                            }
                            else {
                                info("***Error*** Ping node fail [nodeId=" + item.id().toString().toUpperCase() +
                                    ", pingResult=" + flag + ']');
                            }
                        }
                    }
                }
                catch (Exception e) {
                    error("Can't get remote nodes list.", e);
                }

                synchronized (mux) {
                    if (!canceled) {
                        try {
                            mux.wait(2000);
                        }
                        catch (InterruptedException e) {
                            //No-op.
                        }
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            synchronized (mux) {
                canceled = true;

                mux.notifyAll();
            }

            super.interrupt();
        }
    }

    /** */
    private class DiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            info("Discovery event [event=" + evt + ']');
        }
    }

    /** */
    protected AbstractDiscoveryRandomStartStopTest() {
        super(false);
    }

    /**
     * @return Max interval.
     */
    protected int getMaxInterval() {
        return DFLT_MAX_INTERVAL;
    }

    /**
     *
     */
    private class Waiter extends Thread {
        /** {@inheritDoc} */
        @Override public void run() {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("Test started."),
                    new JLabel("Press OK to stop test.")
                },
                "Ignite",
                JOptionPane.INFORMATION_MESSAGE
            );

            semaphore = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    public void testDiscovery() throws Exception {
        Random rand = new Random();

        new Waiter().start();

        while (semaphore) {
            int interval = rand.nextInt(getMaxInterval() - 1) + 1;

            toggleState();

            if (isRunning)
                info("Spi stopped for the interval of " + interval + " seconds...");
            else
                info("Spi started for the interval of " + interval + " seconds...");

            try {
                Thread.sleep(interval * 1000);
            }
            catch (InterruptedException e) {
                error("Got interrupted", e);

                break;
            }
        }

        info("Spi stopped...");
    }

    /**
     * @throws Exception If failed.
     */
    private void toggleState() throws Exception {
        if (isRunning)
            spiStop();
        else
            spiStart();

        isRunning = !isRunning;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        if (getSpiContext() != null)
            getSpiContext().addLocalEventListener(new DiscoveryListener());

        pinger = new Pinger();

        pinger.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        pinger.interrupt();
    }

    /** {@inheritDoc} */
    @Override protected Map<String, Serializable> getNodeAttributes() {
        Map<String, Serializable> attrs = new HashMap<>(1);

        attrs.put("testDiscoveryAttribute", new Date());

        return attrs;
    }
}