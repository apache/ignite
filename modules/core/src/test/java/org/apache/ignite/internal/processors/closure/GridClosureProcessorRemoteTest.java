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

package org.apache.ignite.internal.processors.closure;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests execution of anonymous closures on remote nodes.
 */
@GridCommonTest(group = "Closure Processor")
public class GridClosureProcessorRemoteTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridClosureProcessorRemoteTest() {
        super(true); // Start grid.
    }

    /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        return cfg;
    }

    /**
     * @throws Exception Thrown in case of failure.
     */
    public void testAnonymousBroadcast() throws Exception {
        Ignite g = grid();

        assert g.cluster().nodes().size() >= 2;

        g.compute().run(new CA() {
            @Override public void apply() {
                System.out.println("BROADCASTING....");
            }
        });

        Thread.sleep(2000);
    }

    /**
     * @throws Exception Thrown in case of failure.
     */
    public void testAnonymousUnicast() throws Exception {
        Ignite g = grid();

        assert g.cluster().nodes().size() >= 2;

        ClusterNode rmt = F.first(g.cluster().forRemotes().nodes());

        compute(g.cluster().forNode(rmt)).run(new CA() {
            @Override public void apply() {
                System.out.println("UNICASTING....");
            }
        });

        Thread.sleep(2000);
    }

    /**
     *
     * @throws Exception Thrown in case of failure.
     */
    public void testAnonymousUnicastRequest() throws Exception {
        Ignite g = grid();

        assert g.cluster().nodes().size() >= 2;

        ClusterNode rmt = F.first(g.cluster().forRemotes().nodes());
        final ClusterNode loc = g.cluster().localNode();

        compute(g.cluster().forNode(rmt)).run(new CA() {
            @Override public void apply() {
                message(grid().cluster().forNode(loc)).localListen(new IgniteBiPredicate<UUID, String>() {
                    @Override public boolean apply(UUID uuid, String s) {
                        System.out.println("Received test message [nodeId: " + uuid + ", s=" + s + ']');

                        return false;
                    }
                }, null);
            }
        });

        message(g.cluster().forNode(rmt)).send(null, "TESTING...");

        Thread.sleep(2000);
    }
}