package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This exception is used to indicate error with grid topology (e.g., crashed node, etc.).
 */
public class ClusterTopologyLocalException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new topology exception with given error message.
     *
     * @param msg Error message.
     */
    public ClusterTopologyLocalException(String msg) {
        super(msg);
    }

    /**
     * Creates new topology exception with given error message and optional
     * nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ClusterTopologyLocalException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     *
     * @return Retry ready future.
     */
    public ClusterTopologyCheckedException toChecked(@NotNull IgniteInternalFuture<?> readyFut){
        return new ClusterTopologyCheckedException(this.getMessage(), this.getCause(), readyFut);
    }
}
