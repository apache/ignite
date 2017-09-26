package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

public abstract class GridCacheSetClientAbstractTest extends IgniteCollectionAbstractTest {
    protected static final String SET_NAME = "testSet";

    @Override protected int gridCount() {
        return 2;
    }

    public void testClientJoinsAndCreatesSet() throws Exception {

        IgniteConfiguration clientCfg = getConfiguration();
        clientCfg.setClientMode(true);
        clientCfg.setIgniteInstanceName("client");
        IgniteEx client = startGrid(clientCfg);

        IgniteSet<String> set = client.set(SET_NAME, collectionConfiguration());

        assert set != null;

        set.add("Test");
        assert set.iterator().hasNext();
    }
}
