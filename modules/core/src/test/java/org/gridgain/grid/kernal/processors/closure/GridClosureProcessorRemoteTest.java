/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.closure;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
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
    @Override protected GridConfiguration getConfiguration() throws Exception {
        GridConfiguration cfg = new GridConfiguration();

        cfg.setDiscoverySpi(new GridTcpDiscoverySpi());

        return cfg;
    }

    /**
     * @throws Exception Thrown in case of failure.
     */
    public void testAnonymousBroadcast() throws Exception {
        Grid g = grid();

        assert g.nodes().size() >= 2;

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
        Grid g = grid();

        assert g.nodes().size() >= 2;

        GridNode rmt = F.first(g.forRemotes().nodes());

        g.forNode(rmt).compute().run(new CA() {
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
        Grid g = grid();

        assert g.nodes().size() >= 2;

        GridNode rmt = F.first(g.forRemotes().nodes());
        final GridNode loc = g.localNode();

        g.forNode(rmt).compute().run(new CA() {
            @Override public void apply() {
                grid().forNode(loc).message().localListen(new GridBiPredicate<UUID, String>() {
                    @Override public boolean apply(UUID uuid, String s) {
                        System.out.println("Received test message [nodeId: " + uuid + ", s=" + s + ']');

                        return false;
                    }
                }, null);
            }
        });

        g.forNode(rmt).message().send(null, "TESTING...");

        Thread.sleep(2000);
    }
}
