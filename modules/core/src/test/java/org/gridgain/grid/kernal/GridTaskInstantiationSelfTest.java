/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Tests instantiation of various task types (defined as private inner class, without default constructor, non-public
 * default constructor).
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskInstantiationSelfTest extends GridCommonAbstractTest {
    /**
     * Constructor.
     */
    public GridTaskInstantiationSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If an error occurs.
     */
    public void testTasksInstantiation() throws Exception {
        grid().compute().execute(PrivateClassTask.class, null);

        grid().compute().execute(NonPublicDefaultConstructorTask.class, null);

        try {
            grid().compute().execute(NoDefaultConstructorTask.class, null);

            assert false : "Exception should have been thrown.";
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * Test task defined as private inner class.
     */
    private static class PrivateClassTask extends ComputeTaskAdapter<String, Object> {
        /** */
        @GridLocalNodeIdResource
        private UUID locId;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable String arg) throws GridException {
            for (ClusterNode node : subgrid)
                if (node.id().equals(locId))
                    return Collections.singletonMap(new ComputeJobAdapter() {
                        @Override public Serializable execute() {
                            return null;
                        }
                    }, node);

            throw new GridException("Local node not found.");
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test task defined with non-public default constructor.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class NonPublicDefaultConstructorTask extends PrivateClassTask {
        /**
         * No-op constructor.
         */
        private NonPublicDefaultConstructorTask() {
            // No-op.
        }
    }

    /**
     * Test task defined without default constructor.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class NoDefaultConstructorTask extends PrivateClassTask {
        /**
         * No-op constructor.
         *
         * @param param Some parameter.
         */
        @SuppressWarnings({"unused"})
        private NoDefaultConstructorTask(Object param) {
            // No-op.
        }
    }
}
