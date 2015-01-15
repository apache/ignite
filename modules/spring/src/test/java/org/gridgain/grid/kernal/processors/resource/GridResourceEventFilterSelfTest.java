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
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.springframework.context.support.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.resource.GridAbstractUserResource.*;
import static org.gridgain.grid.kernal.processors.resource.GridResourceTestUtils.*;

/**
 * Test resource injection for event filters.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
public class GridResourceEventFilterSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If test failed.
     */
    public void testCustomFilter1() throws Exception {
        resetResourceCounters();

        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            // Executes task and creates events
            ignite1.compute().execute(TestTask.class, null);

            List<IgniteEvent> evts = ignite1.events().remoteQuery(new CustomEventFilter1(), 0);

            assert !F.isEmpty(evts);

            checkUsageCount(createClss, UserResource1.class, 2);
            checkUsageCount(deployClss, UserResource1.class, 2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }

        checkUsageCount(undeployClss, UserResource1.class, 2);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testCustomFilter2() throws Exception {
        resetResourceCounters();

        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            // Executes task and creates events.
            ignite1.compute().execute(TestTask.class, null);

            List<IgniteEvent> evts = ignite1.events().remoteQuery(new CustomEventFilter2(), 0);

            assert evts != null;
            assert evts.size() == 3;

            // Checks event list. It should have only GridTaskEvent.
            for (IgniteEvent evt : evts) {
                assert evt instanceof IgniteTaskEvent;
            }

            checkUsageCount(createClss, UserResource1.class, 2);
            checkUsageCount(deployClss, UserResource1.class, 2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }

        checkUsageCount(undeployClss, UserResource1.class, 2);
    }

    /**
     * Simple resource class.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class UserResource1 extends GridAbstractUserResource {
        // No-op.
    }

    /**
     * Simple event filter.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class CustomEventFilter1 implements IgnitePredicate<IgniteEvent> {
        /** User resource. */
        @SuppressWarnings("unused")
        @IgniteUserResource
        private transient UserResource1 rsrc;

        /** Grid ID. */
        @SuppressWarnings("unused")
        @IgniteLocalNodeIdResource
        private UUID gridId;

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            return true;
        }
    }

    /**
     * Simple event filter.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class CustomEventFilter2 implements IgnitePredicate<IgniteEvent> {
        /** User resource. */
        @SuppressWarnings("unused")
        @IgniteUserResource
        private transient UserResource1 rsrc;

        /** Logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            if (evt instanceof IgniteTaskEvent) {
                log.info("Received task event: [evt=" + evt + ']');

                return true;
            }

            return false;
        }
    }

    /**
     * Simple task.
     */
    @SuppressWarnings({"PublicInnerClass"})
    @ComputeTaskName("name")
    public static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws IgniteCheckedException {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}
