package org.apache.ignite.internal.processors.service;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.ExecutorService;

/**
 */
public class InactiveOnStartNodeTest extends GridCommonAbstractTest {
    /** Client id. */
    public static final int CLIENT_ID = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setActiveOnStart(false);

        if (getTestGridName(CLIENT_ID).equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    public void testClientActive() throws Exception {
        try {
            IgniteEx srv = startGrid(0);
            IgniteEx client = startGrid(1);

            ExecutorService depExe = GridTestUtils.getFieldValue(client.context().service(),
                GridServiceProcessor.class, "depExe");
            ExecutorService depExeSrv = GridTestUtils.getFieldValue(srv.context().service(),
                GridServiceProcessor.class, "depExe");

            client.active(true);

            ExecutorService depExeActive = GridTestUtils.getFieldValue(client.context().service(),
                GridServiceProcessor.class, "depExe");
            ExecutorService depExeActiveSrv = GridTestUtils.getFieldValue(srv.context().service(),
                GridServiceProcessor.class, "depExe");

            Thread.sleep(10_000);

            srv.close();
            client.close();

            assertTrue(depExeActive.isShutdown());
            assertTrue(depExeActiveSrv.isShutdown());
            assertNull(depExe);
            assertNull(depExeSrv);

        } finally {
            stopAllGrids();
        }
    }

    public void testServerActive() throws Exception {
        try {
            IgniteEx srv = startGrid(0);
            IgniteEx client = startGrid(1);

            ExecutorService depExe = GridTestUtils.getFieldValue(client.context().service(),
                GridServiceProcessor.class, "depExe");
            ExecutorService depExeSrv = GridTestUtils.getFieldValue(srv.context().service(),
                GridServiceProcessor.class, "depExe");

            srv.active(true);

            ExecutorService depExeActive = GridTestUtils.getFieldValue(client.context().service(),
                GridServiceProcessor.class, "depExe");
            ExecutorService depExeActiveSrv = GridTestUtils.getFieldValue(srv.context().service(),
                GridServiceProcessor.class, "depExe");

            Thread.sleep(10_000);

            srv.close();
            client.close();

            assertTrue(depExeActive.isShutdown());
            assertTrue(depExeActiveSrv.isShutdown());
            assertNull(depExe);
            assertNull(depExeSrv);

        } finally {
            stopAllGrids();
        }
    }

}
