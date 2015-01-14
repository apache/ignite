/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
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

            Class<? extends ComputeTask<Object, Object>> taskCls1 =
                (Class<? extends ComputeTask<Object, Object>>)tstClsLdr.loadClass(
                GridUserExternalResourceTask1.class.getName());

            Class<? extends ComputeTask<Object, Object>> taskCls2 =
                (Class<? extends ComputeTask<Object, Object>>)tstClsLdr.loadClass(
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
