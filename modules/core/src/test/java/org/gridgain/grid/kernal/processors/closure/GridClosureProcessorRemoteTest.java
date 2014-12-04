/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.closure;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import java.util.*;

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

        cfg.setDiscoverySpi(new GridTcpDiscoverySpi());

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
                message(grid().forNode(loc)).localListen(new IgniteBiPredicate<UUID, String>() {
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
