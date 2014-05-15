/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.lang.*;

/**
 * Communication with grid in the same process.
 */
public class NewGridGgfsHadoopInProc implements NewGridGgfsHadoop {
    /** Target GGFS. */
    private GridGgfsEx ggfs;

    /**
     * COnstructor.
     *
     * @param ggfs Target GGFS.
     */
    public NewGridGgfsHadoopInProc(GridGgfsEx ggfs) {
        this.ggfs = ggfs;
    }

    /** {@inheritDoc} */
    @Override public GridPlainFuture<GridGgfsHandshakeResponse> handshake(String logDir) {
        // TODO

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO.
    }
}
