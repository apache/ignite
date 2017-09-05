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

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class GridServiceDeploymentCompoundFutureSelfTest extends GridCommonAbstractTest {
    /** */
    private static GridKernalContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteKernal kernal = (IgniteKernal)startGrid(0);

        ctx = kernal.context();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitForCompletionOnFailingFuturePartial() throws Exception {
        GridServiceDeploymentCompoundFuture compFut = new GridServiceDeploymentCompoundFuture(false, ctx);

        int failingFutsNum = 2;

        int completingFutsNum = 5;

        Collection<GridServiceDeploymentFuture> failingFuts = new ArrayList<>(completingFutsNum);

        for (int i = 0; i < failingFutsNum; i++) {
            ServiceConfiguration failingCfg = config("Failed-" + i);

            GridServiceDeploymentFuture failingFut = new GridServiceDeploymentFuture(failingCfg);

            failingFuts.add(failingFut);

            compFut.add(failingFut);
        }

        List<GridFutureAdapter<Object>> futs = new ArrayList<>(completingFutsNum);

        for (int i = 0; i < completingFutsNum; i++) {
            GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(config(String.valueOf(i)));

            futs.add(fut);

            compFut.add(fut);
        }

        compFut.serviceDeploymentMarkInitialized();

        List<Exception> causes = new ArrayList<>();

        for (GridServiceDeploymentFuture fut : failingFuts) {
            Exception cause = new Exception("Test error");

            causes.add(cause);

            fut.onDone(cause);
        }

        try {
            compFut.get(100);

            fail("Should never reach here.");
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            log.info("Expected exception: " + e.getMessage());
        }

        for (GridFutureAdapter<Object> fut : futs)
            fut.onDone();

        try {
            compFut.get();

            fail("Should never reach here.");
        }
        catch (IgniteCheckedException ce) {
            log.info("Expected exception: " + ce.getMessage());

            IgniteException e = U.convertException(ce);

            assertTrue(e instanceof ServiceDeploymentException);

            Throwable[] supErrs = e.getSuppressed();

            assertEquals(failingFutsNum, supErrs.length);

            for (int i = 0; i < failingFutsNum; i++)
                assertEquals(causes.get(i), supErrs[i].getCause());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testFailAllAfterInitialized() throws Exception {
        GridServiceDeploymentCompoundFuture compFut = new GridServiceDeploymentCompoundFuture(true, ctx);

        ServiceConfiguration failingCfg = config("Failed");

        GridServiceDeploymentFuture failingFut = new GridServiceDeploymentFuture(failingCfg);

        compFut.add(failingFut);

        int futsNum = 5;

        List<ServiceConfiguration> cfgs = new ArrayList<>(futsNum + 1);

        cfgs.add(failingCfg);

        for (int i = 0; i < futsNum; i++) {
            ServiceConfiguration cfg = config(String.valueOf(i));

            cfgs.add(cfg);

            compFut.add(new GridServiceDeploymentFuture(cfg));
        }

        compFut.serviceDeploymentMarkInitialized();

        Exception expCause = new Exception("Test error");

        failingFut.onDone(expCause);

        assertFailAll(compFut, cfgs, expCause);
    }

    /**
     * @throws Exception if failed.
     */
    public void testFailAllBeforeInitialized() throws Exception {
        GridServiceDeploymentCompoundFuture compFut = new GridServiceDeploymentCompoundFuture(true, ctx);

        ServiceConfiguration failingCfg = config("Failed");

        GridServiceDeploymentFuture failingFut = new GridServiceDeploymentFuture(failingCfg);

        Exception expCause = new Exception("Test error");

        failingFut.onDone(expCause);

        compFut.add(failingFut);

        assertFalse(compFut.isDone());

        int futsNum = 5;

        List<ServiceConfiguration> cfgs = new ArrayList<>(futsNum + 1);

        cfgs.add(failingCfg);

        for (int i = 0; i < futsNum; i++) {
            ServiceConfiguration cfg = config(String.valueOf(i));

            cfgs.add(cfg);

            compFut.add(new GridServiceDeploymentFuture(cfg));
        }

        compFut.serviceDeploymentMarkInitialized();

        assertFailAll(compFut, cfgs, expCause);
    }

    /**
     * Try waiting for the future completion and check that a proper exception is thrown.
     *
     * @param fut Future.
     * @param expCfgs Expected cfgs.
     * @param expCause Expected cause.
     */
    private void assertFailAll(GridServiceDeploymentCompoundFuture fut, Collection<ServiceConfiguration> expCfgs,
        Exception expCause) {
        try {
            fut.get();

            fail("Should never reach here.");
        }
        catch (IgniteCheckedException ce) {
            log.info("Expected exception: " + ce.getMessage());

            IgniteException e = U.convertException(ce);

            assertTrue(e instanceof ServiceDeploymentException);

            assertEqualsCollections(expCfgs, ((ServiceDeploymentException)e).getFailedConfigurations());

            Throwable actCause = e.getCause();

            assertTrue(actCause instanceof IgniteCheckedException);

            assertEquals(expCause, actCause.getCause());
        }
    }

    /**
     * @param name Name.
     * @return Dummy configuration with a specified name.
     */
    private ServiceConfiguration config(String name) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);

        return cfg;
    }
}
