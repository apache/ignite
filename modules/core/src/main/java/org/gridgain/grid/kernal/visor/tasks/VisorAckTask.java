/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Ack task to run on node.
 */
@GridInternal
public class VisorAckTask extends VisorMultiNodeTask<String, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorAckJob job(String arg) {
        return new VisorAckJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }

    /**
     * Ack job to run on node.
     */
    private static class VisorAckJob extends VisorJob<String, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Message to ack in node console.
         */
        private VisorAckJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(String arg) throws GridException {
            System.out.println("<visor>: ack: " + (arg == null ? g.localNode().id() : arg));

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorAckJob.class, this);
        }
    }
}
