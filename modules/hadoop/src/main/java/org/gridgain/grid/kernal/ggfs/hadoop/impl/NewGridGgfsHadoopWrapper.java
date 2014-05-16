/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.apache.commons.logging.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.ggfs.GridGgfsConfiguration.*;

/**
 * Wrapper for GGFS server.
 */
public class NewGridGgfsHadoopWrapper implements NewGridGgfsHadoop {
    /** Delegate. */
    private final AtomicReference<NewGridGgfsHadoopEx> delegate = new AtomicReference<>();

    /** Connection string. */
    private final NewGridGgfsHadoopEndpoint endpoint;

    /** Logger. */
    private final Log log;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param connStr Connection string.
     */
    NewGridGgfsHadoopWrapper(Log log, String connStr) throws IOException {
        this.endpoint = new NewGridGgfsHadoopEndpoint(connStr);
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHandshakeResponse handshake(final String logDir) throws GridException, IOException {
        return withReconnectHandling(new GridClosureX<NewGridGgfsHadoop, GridGgfsHandshakeResponse>() {
            @Override public GridGgfsHandshakeResponse applyx(NewGridGgfsHadoop delegate)
                throws GridException {
                return null; // TODO.
            }
        });
    }

    /**
     * Execute closure.
     *
     * @param clo Closure.
     * @return Result.
     * @throws IOException If failed.
     */
    private <T> T withReconnectHandling(final GridClosureX<NewGridGgfsHadoop, T> clo) throws IOException {
        Exception err = null;

        for (int i = 0; i < 2; i++) {
            NewGridGgfsHadoopEx curDelegate = null;

            try {
                curDelegate = delegate();

                return clo.applyx(curDelegate);
            }
            catch (GridGgfsIoException e) {
                // Always force close to remove from cache.
                curDelegate.close();

                delegate.compareAndSet(curDelegate, null);

                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (GridException e) {
                throw new IOException(e);
            }
        }

        throw new IOException("Failed to establish connection with GGFS.", err);
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private NewGridGgfsHadoopEx delegate() throws IOException {
        // 1. If delegate is set, return it immediately.
        NewGridGgfsHadoopEx curDelegate = delegate.get();

        if (curDelegate != null)
            return curDelegate;

        // 2. Guess that we are in the same VM.
        GridGgfsEx ggfs = null;

        if (endpoint.grid() == null) {
            try {
                Grid grid = G.grid();

                ggfs = (GridGgfsEx)grid.ggfs(endpoint.ggfs());
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
        else {
            for (Grid grid : G.allGrids()) {
                try {
                    ggfs = (GridGgfsEx)grid.ggfs(endpoint.ggfs());

                    break;
                }
                catch (Exception ignore) {
                    // No-op.
                }
            }
        }

        if (ggfs != null)
            return new NewGridGgfsHadoopInProc(ggfs, log);

        // 3. Try connecting using shmem.
        if (!U.isWindows()) {
            // TODO.
        }

        // 4. Try loopback TCP connection.
        // TODO.

        // 5. Try remote TCP connection.
        new NewGridGgfsHadoopOutProc(log, ""); // TODO: Proper arguments here.

        throw new IOException("Failed to connect: " + endpoint);
    }
}
