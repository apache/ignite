/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Interface for all main internal GridGain components (managers and processors).
 */
public interface GridComponent {
    /**
     * Starts grid component.
     *
     * @throws GridException Throws in case of any errors.
     */
    public void start() throws GridException;

    /**
     * Stops grid component.
     *
     * @param cancel If {@code true}, then all ongoing tasks or jobs for relevant
     *      components need to be cancelled.
     * @throws GridException Thrown in case of any errors.
     */
    public void stop(boolean cancel) throws GridException;

    /**
     * Callback that notifies that kernal has successfully started,
     * including all managers and processors.
     *
     * @throws GridException Thrown in case of any errors.
     */
    public void onKernalStart() throws GridException;

    /**
     * Callback to notify that kernal is about to stop.
     *
     * @param cancel Flag indicating whether jobs should be canceled.
     */
    public void onKernalStop(boolean cancel);

    /**
     * Gets discovery data object that will be sent to new node
     * during discovery process.
     *
     * @param nodeId ID of new node that joins topology.
     * @return Discovery data object or {@code null} if there is nothing
     *      to send for this component.
     */
    @Nullable public Object collectDiscoveryData(UUID nodeId);

    /**
     * Receives discovery data object from remote nodes (called
     * on new node during discovery process).
     *
     * @param data Discovery data object or {@code null} if nothing was
     *      sent for this component.
     */
    public void onDiscoveryDataReceived(Object data);

    /**
     * Prints memory statistics (sizes of internal structures, etc.).
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats();

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    @Nullable public ClusterNodeValidationResult validateNode(ClusterNode node);
}
