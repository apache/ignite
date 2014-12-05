/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.security;

import org.jetbrains.annotations.*;

/**
 * Supported security subject types. Subject type can be retrieved form {@link GridSecuritySubject#type()} method.
 */
public enum GridSecuritySubjectType {
    /**
     * Subject type for a remote {@link org.apache.ignite.cluster.ClusterNode}.
     */
    REMOTE_NODE,

    /**
     * Subject type for remote client.
     */
    REMOTE_CLIENT;

    /** Enumerated values. */
    private static final GridSecuritySubjectType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridSecuritySubjectType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
