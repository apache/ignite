/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.gridgain.grid.kernal.ggfs.hadoop.*;

/**
 * Extended GGFS server interface.
 */
public interface NewGridGgfsHadoopEx extends NewGridGgfsHadoop {
    /**
     * @return Mode.
     */
    public NewGridGgfsHadoopMode mode();
}
