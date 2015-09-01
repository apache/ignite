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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.failover.FailoverContext;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

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
        catch (IgniteException e) {
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
                new IgniteException("Task should NOT be failed over"));

            assert false;
        }
        catch (IgniteException e) {
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
            assert arg instanceof IgniteException;

            Collection<ComputeJob> res = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++)
                res.add(new GridTestFailoverJob((IgniteException)arg));

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) {
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
        GridTestFailoverJob(IgniteException ex) { super(ex); }

        /** {@inheritDoc} */
        @Override public IgniteException execute() {
            throw this.<IgniteException>argument(0);
        }
    }
}