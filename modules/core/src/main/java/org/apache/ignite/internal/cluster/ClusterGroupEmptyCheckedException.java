package org.apache.ignite.internal.cluster;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This exception defines illegal call on empty projection. Thrown by projection when operation
 * that requires at least one node is called on empty projection.
 */
public class ClusterGroupEmptyCheckedException extends ClusterTopologyCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with default error message.
     * @param readyFut Retry ready future.
     */
    public ClusterGroupEmptyCheckedException(@NotNull IgniteInternalFuture<?> readyFut) {
        super("Cluster group is empty.", readyFut);
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     * @param readyFut Retry ready future.
     */
    public ClusterGroupEmptyCheckedException(String msg, @NotNull IgniteInternalFuture<?> readyFut) {
        super(msg, readyFut);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     * @param readyFut Retry ready future.
     */
    public ClusterGroupEmptyCheckedException(String msg, @Nullable Throwable cause,
        @NotNull IgniteInternalFuture<?> readyFut) {
        super(msg, cause, readyFut);
    }
}
