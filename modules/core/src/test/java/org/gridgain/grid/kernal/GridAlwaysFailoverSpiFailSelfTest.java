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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.failover.*;
import org.apache.ignite.spi.failover.always.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Always failover SPI test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridAlwaysFailoverSpiFailSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean isFailoverCalled;

    /** */
    public GridAlwaysFailoverSpiFailSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setFailoverSpi(new GridTestFailoverSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedCatchParameter", "ThrowableInstanceNeverThrown"})
    public void testFailoverTask() throws Exception {
        isFailoverCalled = false;

        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTestFailoverTask.class, GridTestFailoverTask.class.getClassLoader());

        try {
            ignite.compute().execute(GridTestFailoverTask.class.getName(),
                new ComputeExecutionRejectedException("Task should be failed over"));

            assert false;
        }
        catch (IgniteCheckedException e) {
            //No-op
        }

        assert isFailoverCalled;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedCatchParameter", "ThrowableInstanceNeverThrown"})
    public void testNoneFailoverTask() throws Exception {
        isFailoverCalled = false;

        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTestFailoverTask.class, GridTestFailoverTask.class.getClassLoader());

        try {
            ignite.compute().execute(GridTestFailoverTask.class.getName(),
                new IgniteCheckedException("Task should NOT be failed over"));

            assert false;
        }
        catch (IgniteCheckedException e) {
            //No-op
        }

        assert !isFailoverCalled;
    }

    /** */
    private class GridTestFailoverSpi extends AlwaysFailoverSpi {
        /** {@inheritDoc} */
        @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> grid) {
            isFailoverCalled = true;

            return super.failover(ctx, grid);
        }
    }

    /**
     * Task which splits to the jobs that always fail.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class GridTestFailoverTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            assert gridSize == 1;
            assert arg instanceof IgniteCheckedException;

            Collection<ComputeJob> res = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++)
                res.add(new GridTestFailoverJob((IgniteCheckedException)arg));

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) throws IgniteCheckedException {
            if (res.getException() != null)
                return ComputeJobResultPolicy.FAILOVER;

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Job that always throws exception.
     */
    private static class GridTestFailoverJob extends ComputeJobAdapter {
        /**
         * @param ex Exception to be thrown in {@link #execute}.
         */
        GridTestFailoverJob(IgniteCheckedException ex) { super(ex); }

        /** {@inheritDoc} */
        @Override public IgniteCheckedException execute() throws IgniteCheckedException {
            throw this.<IgniteCheckedException>argument(0);
        }
    }
}
