/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Internal task session interface.
 */
public interface GridTaskSessionInternal extends GridComputeTaskSession {
    /**
     * @return Checkpoint SPI name.
     */
    public String getCheckpointSpi();

    /**
     * @return Job ID (possibly <tt>null</tt>).
     */
    @Nullable public GridUuid getJobId();

    /**
     * @return {@code True} if task node.
     */
    public boolean isTaskNode();

    /**
     * Closes session.
     */
    public void onClosed();

    /**
     * @return Checks if session is closed.
     */
    public boolean isClosed();

    /**
     * @return Task session.
     */
    public GridTaskSessionInternal session();

    /**
     * @return {@code True} if checkpoints and attributes are enabled.
     */
    public boolean isFullSupport();

    /**
     * @return Subject ID.
     */
    public UUID subjectId();
}
