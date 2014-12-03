/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.deployment.uri.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Test what happens if peer class loading is disabled.
 * <p>
 * In order for this test to run, make sure that your
 * {@code p2p.uri.cls} folder ends with {@code .gar} extension.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown"})
@GridCommonTest(group = "P2P")
public class GridP2PDisabledSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** External class loader. */
    private static ClassLoader extLdr;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** */
    private boolean initGar;

    /** Path to GAR file. */
    private String garFile;

    /**
     *
     */
    public GridP2PDisabledSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        extLdr = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDeploymentMode(depMode);

        if (initGar) {
            GridUriDeploymentSpi depSpi = new GridUriDeploymentSpi();

            depSpi.setUriList(Collections.singletonList(garFile));

            cfg.setDeploymentSpi(depSpi);
        }

        ((GridTcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

        return cfg;
    }

    /**
     * Test what happens if peer class loading is disabled.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void checkClassNotFound() throws Exception {
        initGar = false;

        try {
            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            Class task = extLdr.loadClass(TASK_NAME);

            try {
                grid1.compute().execute(task, grid2.cluster().localNode().id());

                assert false;
            }
            catch (GridException e) {
                info("Received expected exception: " + e);
            }
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void checkGar() throws Exception {
        initGar = true;

        String garDir = "modules/extdata/p2p/deploy";
        String garFileName = "p2p.gar";

        File origGarPath = U.resolveGridGainPath(garDir + '/' + garFileName);

        File tmpPath = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        if (!tmpPath.mkdir())
            throw new IOException("Can not create temp directory");

        try {
            File newGarFile = new File(tmpPath, garFileName);

            U.copy(origGarPath, newGarFile, false);

            assert newGarFile.exists();

            try {
                garFile = "file:///" + tmpPath.getAbsolutePath();

                try {
                    Grid grid1 = startGrid(1);
                    Grid grid2 = startGrid(2);

                    int[] res = grid1.compute().<UUID, int[]>execute(TASK_NAME, grid2.cluster().localNode().id());

                    assert res != null;
                    assert res.length == 2;
                }
                finally {
                    stopGrid(1);
                    stopGrid(2);
                }
            }
            finally {
                if (newGarFile != null && !newGarFile.delete())
                    error("Can not delete temp gar file");
            }
        }
        finally {
            if (!tmpPath.delete())
                error("Can not delete temp directory");
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarPrivateMode() throws Exception {
        depMode = GridDeploymentMode.PRIVATE;

        checkGar();
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarIsolatedMode() throws Exception {
        depMode = GridDeploymentMode.ISOLATED;

        checkGar();
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarContinuousMode() throws Exception {
        depMode = GridDeploymentMode.CONTINUOUS;

        checkGar();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        checkGar();
    }
}
