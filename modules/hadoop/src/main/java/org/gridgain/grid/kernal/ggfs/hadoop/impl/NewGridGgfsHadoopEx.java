/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;

import java.io.*;

/**
 * Extended GGFS server interface.
 */
public interface NewGridGgfsHadoopEx extends NewGridGgfsHadoop {
    /**
     * @return Mode.
     */
    public NewGridGgfsHadoopMode mode();

    /**
     * Adds event listener that will be invoked when connection with server is lost or remote error has occurred.
     * If connection is closed already, callback will be invoked synchronously inside this method.
     *
     * @param desc Stream descriptor.
     * @param lsnr Event listener.
     */
    public void addEventListener(NewGridGgfsHadoopStreamDelegate desc, GridGgfsHadoopStreamEventListener lsnr);

    /**
     * Removes event listener that will be invoked when connection with server is lost or remote error has occurred.
     *
     * @param desc Stream descriptor.
     */
    public void removeEventListener(NewGridGgfsHadoopStreamDelegate desc);
}
