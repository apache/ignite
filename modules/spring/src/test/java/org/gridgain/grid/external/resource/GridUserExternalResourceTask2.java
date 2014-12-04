/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.external.resource;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.external.resource.GridAbstractUserExternalResource.*;

/**
 * Resource injection task.
 */
@SuppressWarnings("PublicInnerClass")
public class GridUserExternalResourceTask2 extends GridComputeTaskSplitAdapter<Object, Object> {
    /** User resource. */
    @GridUserResource private transient GridUserExternalResource1 rsrc1;

    /** User resource. */
    @GridUserResource(resourceClass = GridUserExternalResource2.class)
    private transient GridAbstractUserExternalResource rsrc2;

    /** User resource. */
    @GridUserResource(resourceName = "rsrc3")
    private transient GridUserExternalResource1 rsrc3;

    /** User resource. */
    @GridUserResource(resourceClass = GridUserExternalResource2.class, resourceName = "rsrc4")
    private transient GridAbstractUserExternalResource rsrc4;

    /** */
    @GridLoggerResource private GridLogger log;

    /** */
    @GridTaskSessionResource private GridComputeTaskSession ses;

    /** {@inheritDoc} */
    @Override protected Collection<GridComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
        assert rsrc1 != null;
        assert rsrc2 != null;
        assert rsrc3 != null;
        assert rsrc4 != null;
        assert log != null;
        assert ses != null;

        checkUsageCount(createClss, GridUserExternalResource1.class, 2);
        checkUsageCount(createClss, GridUserExternalResource2.class, 2);
        checkUsageCount(deployClss, GridUserExternalResource1.class, 2);
        checkUsageCount(deployClss, GridUserExternalResource2.class, 2);

        log.info("Injected shared resource1 into task: " + rsrc1);
        log.info("Injected shared resource2 into task: " + rsrc2);
        log.info("Injected shared resource3 into task: " + rsrc3);
        log.info("Injected shared resource4 into task: " + rsrc4);

        Collection<GridComputeJobAdapter> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++)
            jobs.add(new GridUserExternalResourceJob2());

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        assert rsrc1 != null;
        assert rsrc2 != null;
        assert rsrc3 != null;
        assert rsrc4 != null;
        assert log != null;

        checkUsageCount(createClss, GridUserExternalResource1.class, 2);
        checkUsageCount(createClss, GridUserExternalResource2.class, 2);
        checkUsageCount(deployClss, GridUserExternalResource1.class, 2);
        checkUsageCount(deployClss, GridUserExternalResource2.class, 2);

        // Nothing to reduce.
        return null;
    }
    /**
     * Job with injected resources.
     */
    public final class GridUserExternalResourceJob2 extends GridComputeJobAdapter {
        /** User resource. */
        @GridUserResource(resourceClass = GridUserExternalResource1.class)
        private transient GridAbstractUserExternalResource rsrc5;

        /** User resource. */
        @GridUserResource private transient GridUserExternalResource2 rsrc6;

        /** User resource. */
        @GridUserResource(resourceClass = GridUserExternalResource1.class, resourceName = "rsrc3")
        private transient GridAbstractUserExternalResource rsrc7;

        /** User resource. */
        @GridUserResource(resourceName = "rsrc4")
        private transient GridUserExternalResource2 rsrc8;

        /** */
        @GridLocalNodeIdResource private UUID locId;

        /** {@inheritDoc} */
        @SuppressWarnings({"ObjectEquality"})
        @Override public Serializable execute() {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;
            assert locId != null;

            assert rsrc5 != null;
            assert rsrc6 != null;
            assert rsrc7 != null;
            assert rsrc8 != null;

            // Make sure that neither task nor global scope got
            // created more than once.
            assert rsrc1 == rsrc5;
            assert rsrc2 == rsrc6;
            assert rsrc3 == rsrc7;
            assert rsrc4 == rsrc8;

            // According to the UserResource class description
            // different tasks should have different resources deployed.
            // Thus second task (this one) should have 4 created/deployed resources.
            checkUsageCount(createClss, GridUserExternalResource1.class, 2);
            checkUsageCount(createClss, GridUserExternalResource2.class, 2);
            checkUsageCount(deployClss, GridUserExternalResource1.class, 2);
            checkUsageCount(deployClss, GridUserExternalResource2.class, 2);

            log.info("Injected shared resource1 into job: " + rsrc1);
            log.info("Injected shared resource2 into job: " + rsrc2);
            log.info("Injected shared resource3 into job: " + rsrc3);
            log.info("Injected shared resource4 into job: " + rsrc4);
            log.info("Injected shared resource5 into job: " + rsrc5);
            log.info("Injected shared resource6 into job: " + rsrc6);
            log.info("Injected shared resource7 into job: " + rsrc7);
            log.info("Injected shared resource8 into job: " + rsrc8);
            log.info("Injected log resource into job: " + log);

            return null;
        }
    }
}
