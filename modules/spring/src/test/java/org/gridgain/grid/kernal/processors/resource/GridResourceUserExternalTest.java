/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.external.resource.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.springframework.context.support.*;

/**
 *
 */
@GridCommonTest(group = "Resource Self")
public class GridResourceUserExternalTest extends GridCommonAbstractTest {
    /** */
    public GridResourceUserExternalTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        c.setPeerClassLoadingLocalClassPathExclude(
            GridUserExternalResourceTask1.class.getName(),
            GridUserExternalResourceTask2.class.getName(),
            GridUserExternalResourceTask1.GridUserExternalResourceJob1.class.getName(),
            GridUserExternalResourceTask2.GridUserExternalResourceJob2.class.getName(),
            GridAbstractUserExternalResource.class.getName(),
            GridUserExternalResource1.class.getName(),
            GridUserExternalResource2.class.getName()
        );

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testExternalResources() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;

        try {
            ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            GridTestClassLoader tstClsLdr = new GridTestClassLoader(null, getClass().getClassLoader(),
                GridUserExternalResourceTask1.class.getName(),
                GridUserExternalResourceTask2.class.getName(),
                GridUserExternalResourceTask1.GridUserExternalResourceJob1.class.getName(),
                GridUserExternalResourceTask2.GridUserExternalResourceJob2.class.getName(),
                GridAbstractUserExternalResource.class.getName(),
                GridUserExternalResource1.class.getName(),
                GridUserExternalResource2.class.getName());

            Class<? extends GridComputeTask<Object, Object>> taskCls1 =
                (Class<? extends GridComputeTask<Object, Object>>)tstClsLdr.loadClass(
                GridUserExternalResourceTask1.class.getName());

            Class<? extends GridComputeTask<Object, Object>> taskCls2 =
                (Class<? extends GridComputeTask<Object, Object>>)tstClsLdr.loadClass(
                GridUserExternalResourceTask2.class.getName());

            // Execute the same task twice.
            ignite1.compute().execute(taskCls1, null);
            ignite1.compute().execute(taskCls2, null);
        }
        finally {
            GridTestUtils.close(ignite1, log());
            GridTestUtils.close(ignite2, log());
        }
    }
}
