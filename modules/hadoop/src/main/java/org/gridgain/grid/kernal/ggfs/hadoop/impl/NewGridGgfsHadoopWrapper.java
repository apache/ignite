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
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class NewGridGgfsHadoopWrapper {
    /** Delegate. */
    private final AtomicReference<NewGridGgfsHadoop> delegate = new AtomicReference<>();

    /** Connection string. */
    private final String connStr;

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     */
    NewGridGgfsHadoopWrapper(String connStr) {
        this.connStr = connStr;
    }

    /**
     *
     * @param clo
     * @param <T>
     * @return
     * @throws IOException
     */
    private <T> T withReconnectHandling(final GridClosureX<NewGridGgfsHadoop, T> clo) throws IOException {
        Exception err = null;

        for (int i = 0; i < 2; i++) {
            NewGridGgfsHadoop curDelegate = null;

            try {
                curDelegate = ipcIo();

                return c.applyx(locIo);
            }
            catch (GridGgfsIoException e) {
                // Always force close to remove from cache.
                locIo.forceClose();

                clientIo.compareAndSet(locIo, null);

                // Always output in debug.
                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (IOException e) {
                return new GridPlainFutureAdapter<>(e);
            }
            catch (GridException e) {
                return new GridPlainFutureAdapter<>(e);
            }
        }


        return null;
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private NewGridGgfsHadoop delegate() {
        // TODO.

        return null;
    }
}
