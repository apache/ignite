/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.datagrid.hibernate.*;

/**
 * Multi-node test for {@link HibernateL2CacheExample}.
 */
public class GridHibernateL2CacheExampleMultiNodeSelfTest extends GridHibernateL2CacheExampleSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid("node-" + i, HibernateL2CacheExampleNodeStartup.configuration());
    }
}
