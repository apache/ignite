/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Task to start DR full state transfer.
 */
@GridInternal
public class VisorDrSenderCacheBootstrapTask extends VisorOneNodeTask<GridBiTuple<String, byte[]>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorDrSenderCacheBootstrapJob job(GridBiTuple<String, byte[]> arg) {
        return new VisorDrSenderCacheBootstrapJob(arg);
    }

    /**
     * Job that start DR full state transfer.
     */
    private static class VisorDrSenderCacheBootstrapJob extends VisorJob<GridBiTuple<String, byte[]>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Cache name to bootstrap and remote data centers IDs.
         */
        private VisorDrSenderCacheBootstrapJob(GridBiTuple<String, byte[]> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(GridBiTuple<String, byte[]> arg) throws GridException {
            String cacheName = arg.get1();
            byte[] rmtDrs = arg.get2();

            g.dr().senderCacheDrStateTransfer(cacheName, rmtDrs);

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorDrSenderCacheBootstrapJob.class, this);
        }
    }
}
