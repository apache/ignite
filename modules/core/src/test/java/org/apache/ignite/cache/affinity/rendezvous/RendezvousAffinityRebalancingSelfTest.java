package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.AbstractAffinityRebalancingTest;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Created by agura on 03.02.2016.
 */
public class RendezvousAffinityRebalancingSelfTest extends AbstractAffinityRebalancingTest {

    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunction(Ignite ignite) {
        AffinityFunction aff = new RendezvousAffinityFunction();

        GridTestUtils.setFieldValue(aff, "ignite", ignite);

        return aff;
    }
}
