/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * Job sibling interface defines a job from the same split. In other words a sibling is a job returned
 * from the same {@link ComputeTask#map(List, Object)} method invocation.
 */
public interface ComputeJobSibling extends GridMetadataAware {
    /**
     * Gets ID of this grid job sibling. Note that ID stays constant
     * throughout job life time, even if a job gets failed over to another
     * node.
     *
     * @return Job ID.
     */
    public IgniteUuid getJobId();

    /**
     * Sends a request to cancel this sibling.
     *
     * @throws IgniteCheckedException If cancellation message could not be sent.
     */
    public void cancel() throws IgniteCheckedException;
}
