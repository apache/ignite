/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

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
public class NewGridGgfsHadoopWrapper {
    /** Delegate. */
    private final AtomicReference<NewGridGgfsHadoop> delegate = new AtomicReference<>();

    /** Connection string. */
    private final NewGridGgfsHadoopEndpoint endpoint;

    /** Whether to force switch to in-proc mode. */
    private volatile GridGgfsEx forceInner;

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     */
    NewGridGgfsHadoopWrapper(String connStr) throws IOException {
        this.endpoint = new NewGridGgfsHadoopEndpoint(connStr);

        final NewGridGgfsHadoopEndpoint endpoint0 = endpoint;

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);


                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }, "gridgain-hadoop-ggfs-inproc-checker");

        thread.setDaemon(true);

        thread.start();
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
            NewGridGgfsHadoop curDelegate = null;

            try {
                curDelegate = delegate();

                return clo.applyx(curDelegate);
            }
            catch (GridGgfsIoException e) {
                // Always force close to remove from cache.
                curDelegate.close();

                delegate.compareAndSet(curDelegate, null);

                // TODO: Set logger.
//                // Always output in debug.
//                if (log.isDebugEnabled())
//                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (GridException e) {
                throw new IOException(e);
            }
        }

        throw new IOException("Failed to establish connection with GGFS.", err);
    }

    /**
     * Try getting in
     * @param endpoint
     * @return
     */
    @Nullable private static GridGgfsEx inProcGffs(NewGridGgfsHadoopEndpoint endpoint) {
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

        return ggfs;
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private NewGridGgfsHadoop delegate() throws IOException {
        // 1. If delegate is set, return it immediately.
        NewGridGgfsHadoop curDelegate = delegate.get();

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
            return new NewGridGgfsHadoopInProc(ggfs);
        else
            return new NewGridGgfsHadoopOutProc(null, null); // TODO: Proper arguments here.
    }
}
