/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
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
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
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
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
