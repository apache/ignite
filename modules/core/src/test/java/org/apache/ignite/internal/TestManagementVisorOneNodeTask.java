package org.apache.ignite.internal;

import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridVisorManagementTask
public class TestManagementVisorOneNodeTask extends VisorOneNodeTask<VisorTaskArgument, Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorValidNodeJob job(VisorTaskArgument arg) {
        return new VisorValidNodeJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object reduce0(List<ComputeJobResult> results) {
        return null;
    }

    /**
     * Job that ping node.
     */
    private static class VisorValidNodeJob extends VisorJob<VisorTaskArgument, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Node ID to ping.
         * @param debug Debug flag.
         */
        protected VisorValidNodeJob(VisorTaskArgument arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Object run(VisorTaskArgument arg) {
            return null;
        }
    }
}
