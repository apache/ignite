/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.benchmarks.risk.affinity;

import org.gridgain.benchmarks.risk.model.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.junit.*;

import java.util.concurrent.*;

import static org.gridgain.benchmarks.risk.model.GridRiskDataEntry.*;

/**
 *
 */
public class GridRiskPartitionedAffinitySelfTest extends GridCommonAbstractTest {
    /**
     * Test affinity initialization.
     */
    public void testAffinityInitialization() {
        for (int i = 0; i < 5; i++) {
            final GridRiskPartitionedAffinityFunction aff = new GridRiskPartitionedAffinityFunction();

            switch (i % 5) {
                case 0: {
                    aff.setFactor1(FACTOR1);

                    break;
                }

                case 1: {
                    aff.setFactor1(FACTOR2);

                    break;
                }

                case 2: {
                    aff.setFactor2(FACTOR1);

                    break;
                }

                case 3: {
                    aff.setFactor2(FACTOR2);

                    break;
                }

                default: {
                    // No-op.

                    break;
                }
            }

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    aff.getPartitions();

                    return null;
                }
            }, IllegalStateException.class, "Affinity was not properly initialized.");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    aff.getPartitions();

                    return null;
                }
            }, IllegalStateException.class, "Affinity was not properly initialized.");
        }
    }

    /**
     * Test partition calculations.
     */
    public void testAffinity() {
        GridRiskPartitionedAffinityFunction aff = new GridRiskPartitionedAffinityFunction();

        aff.setFactor1(FACTOR1);
        aff.setFactor2(FACTOR2);

        for (int i = 0; i < FACTOR1.size(); i++) {
            for (int j = 0; j < FACTOR2.size(); j++) {
                GridRiskAffinityKey key = new GridRiskAffinityKey(FACTOR1.get(i), FACTOR2.get(j));

                int part = aff.partition(key);

                Assert.assertEquals(i * FACTOR1.size() + j, part);
            }
        }

        Assert.assertEquals(FACTOR1.size() * FACTOR2.size(), aff.getPartitions());
        Assert.assertEquals(FACTOR1.size() * FACTOR2.size(), aff.partitions());
    }
}
