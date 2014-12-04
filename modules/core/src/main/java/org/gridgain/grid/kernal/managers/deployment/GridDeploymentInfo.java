/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.deployment;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * Deployment info.
 */
public interface GridDeploymentInfo {
    /**
     * @return Class loader ID.
     */
    public IgniteUuid classLoaderId();

    /**
     * @return User version.
     */
    public String userVersion();

    /**
     * @return Sequence number.
     */
    public long sequenceNumber();

    /**
     * @return Deployment mode.
     */
    public GridDeploymentMode deployMode();

    /**
     * @return Local deployment ownership flag.
     */
    public boolean localDeploymentOwner();

    /**
     * @return Participant map for SHARED mode.
     */
    public Map<UUID, IgniteUuid> participants();
}
