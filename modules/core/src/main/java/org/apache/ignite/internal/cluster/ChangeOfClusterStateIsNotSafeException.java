package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteException;

/**
 * Warns of possibility to lose data on change of cluster state.
 */
public class ChangeOfClusterStateIsNotSafeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default constructor. */
    public ChangeOfClusterStateIsNotSafeException() {
    }

    /**
     * @param msg Message.
     */
    public ChangeOfClusterStateIsNotSafeException(String msg) {
        super(msg);
    }
}
