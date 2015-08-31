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
import java.util.UUID;
import javax.swing.JOptionPane;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;

/**
 * Base discovery test class.
 * @param <T> SPI implementation class.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public abstract class AbstractDiscoveryTest<T extends DiscoverySpi> extends GridSpiAbstractTest<T> {
    /** */
    @SuppressWarnings({"ClassExplicitlyExtendsThread"})
    private class Pinger extends Thread {
        /** */
        private final Object mux = new Object();

        /** */
        @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
        private boolean isCanceled;

        /** {@inheritDoc} */
        @SuppressWarnings({"UnusedCatchParameter"})
        @Override public void run() {
            Random rnd = new Random();

            while (isCanceled) {
                try {
                    Collection<ClusterNode> nodes = getSpi().getRemoteNodes();

                    pingNode(UUID.randomUUID(), false);

                    for (ClusterNode item : nodes) {
                        pingNode(item.id(), true);
                    }

                    pingNode(UUID.randomUUID(), false);
                }
                catch (Exception e) {
                    error("Can't get SPI.", e);
                }

                synchronized (mux) {
                    if (isCanceled) {
                        try {
                            mux.wait(getPingFrequency() * (1 + rnd.nextInt(10)));
                        }
                        catch (InterruptedException e) {
                            //No-op.
                        }
                    }
                }
            }
        }

        /**
         * @param nodeId Node UUID.
         * @param exists Exists flag.
         * @throws Exception If failed.
         */
        private void pingNode(UUID nodeId, boolean exists) throws Exception {
            boolean flag = getSpi().pingNode(nodeId);

            info((flag != exists ? "***Error*** " : "") + "Ping " + (exists ? "exist" : "random") +
                " node [nodeId=" + nodeId + ", pingResult=" + flag + ']');
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            synchronized (mux) {
                isCanceled = true;

                mux.notifyAll();
            }

            super.interrupt();
        }
    }

    /**
     * @return Ping frequency.
     */
    public abstract long getPingFrequency();

    /**
     * @return Pinger start flag.
     */
    public boolean isPingerStart() {
        return true;
    }

    /** */
    private class DiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            info("Discovery event [event=" + evt + ']');
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiscovery() throws Exception {
        GridLocalEventListener discoLsnr = new DiscoveryListener();

        getSpiContext().addLocalEventListener(discoLsnr);

        Pinger pinger = null;

        if (isPingerStart()) {
            pinger = new Pinger();

            pinger.start();
        }

        JOptionPane.showMessageDialog(null, "Press OK to end test.");

        if (pinger != null)
            pinger.interrupt();
    }

    /** {@inheritDoc} */
    @Override protected Map<String, Serializable> getNodeAttributes() {
        Map<String, Serializable> attrs = new HashMap<>(1);

        attrs.put("testDiscoveryAttribute", new Date());

        return attrs;
    }
}