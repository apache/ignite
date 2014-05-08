/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.datagrid.hibernate.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;

/**
 * Tests the {@link HibernateL2CacheExample}.
 */
public class GridHibernateL2CacheExampleSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridHibernateL2CacheExample() throws Exception {
        File cfgFile = U.resolveGridGainPath("examples/config/hibernate/example-hibernate-L2-cache.xml");

        HibernateL2CacheExample.main(new String[] { cfgFile.getAbsolutePath() });
    }
}
