/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Ack task to run on node.
 */
@GridInternal
public class VisorAckTask extends VisorMultiNodeTask<VisorAckTask.VisorAckArg, Void, Void> {
    /**
     * Ack task argument to run on node.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorAckArg extends VisorMultiNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String msg;

        /**
         * @param msg - generating message function.
         */
        public VisorAckArg(Set<UUID> nids, String msg) {
            super(nids);

            this.msg = msg;
        }

        /**
         * @return Message.
         */
        public String message() {
            return msg;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorAckJob extends VisorJob<VisorAckArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        public VisorAckJob(VisorAckArg arg) {
            super(arg);
        }

        @Override protected Void run(VisorAckArg arg) throws GridException {
            System.out.println("<visor>: ack: " + (arg.msg == null ? g.localNode().id() : arg.msg)); // TODO

            return null;
        }
    }

    @Override protected VisorAckJob job(VisorAckArg arg) {
        return new VisorAckJob(arg);
    }

    @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
