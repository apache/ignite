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
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class GridServiceDeploymentCompoundFutureSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testWaitForCompletionOnFailingFuture() throws Exception {
        GridServiceDeploymentCompoundFuture compFut = new GridServiceDeploymentCompoundFuture();

        int failingFutsNum = 2;

        int completingFutsNum = 5;

        Collection<GridServiceDeploymentFuture> failingFuts = new ArrayList<>(completingFutsNum);

        for (int i = 0; i < failingFutsNum; i++) {
            ServiceConfiguration failingCfg = config("Failed-" + i);

            GridServiceDeploymentFuture failingFut = new GridServiceDeploymentFuture(failingCfg, IgniteUuid.randomUuid());

            failingFuts.add(failingFut);

            compFut.add(failingFut);
        }

        List<GridFutureAdapter<Object>> futs = new ArrayList<>(completingFutsNum);

        for (int i = 0; i < completingFutsNum; i++) {
            GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(config(String.valueOf(i)), IgniteUuid.randomUuid());

            futs.add(fut);

            compFut.add(fut);
        }

        compFut.markInitialized();

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
     * @param name Name.
     * @return Dummy configuration with a specified name.
     */
    private ServiceConfiguration config(String name) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);

        return cfg;
    }
}
