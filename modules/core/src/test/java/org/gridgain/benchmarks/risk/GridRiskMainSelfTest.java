package org.gridgain.benchmarks.risk;

import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.benchmarks.risk.GridRiskMain.*;

/**
 * Risk analytics benchmark self test.
 */
public class GridRiskMainSelfTest extends GridAbstractExamplesTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = loadConfiguration(WORKER_CFG);

        cfg.setGridName(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaults() throws Exception {
        GridRiskMain.main();
    }

    /**
     * @throws Exception If failed.
     */
    public void testApproach1() throws Exception {
        GridRiskMain.main("20000", "5000", MASTER_CFG, "true", "approach1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testApproach2() throws Exception {
        GridRiskMain.main("20000", "5000", MASTER_CFG, "true", "approach2");
    }

    /**
     * @throws Exception If failed.
     */
    public void testApproach1NoMetrics() throws Exception {
        GridRiskMain.main("20000", "5000", MASTER_CFG, "false", "approach1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testApproach2NoMetrics() throws Exception {
        GridRiskMain.main("20000", "5000", MASTER_CFG, "false", "approach2");
    }
}
