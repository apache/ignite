/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;

import java.util.*;

/**
 * Ack task to run on node.
 */
@GridInternal
public class VisorAckTask extends VisorOneNodeTask<VisorAckTask.VisorAckArg, Void> {
    @Override protected GridComputeJob job(VisorAckArg arg) {
        return new VisorAckJob(arg);
    }

    /**
     * Ack task argument to run on node.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorAckArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String msg;

        /**
         * @param msg - generating message function.
         */
        public VisorAckArg(UUID nodeId, String msg) {
            super(nodeId);

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
    public static class VisorAckJob extends VisorOneNodeJob<VisorAckArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        protected VisorAckJob(VisorAckArg arg) {
            super(arg);
        }

        @Override protected Void run(VisorAckArg arg) {
            System.out.println("<visor>: ack: " + arg.msg); // TODO

            return null;
        }
    }
}
