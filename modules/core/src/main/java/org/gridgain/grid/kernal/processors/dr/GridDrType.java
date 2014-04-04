/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

/**
 * Data center replication type.
 */
public enum GridDrType {
    /** Do not replicate that entry. */
    DR_NONE,

    /** Regular replication on primary node. */
    DR_PRIMARY,

    /** Regular replication on backup node. */
    DR_BACKUP,

    /** Replication during load. */
    DR_LOAD,

    /** Replication during preload. */
    DR_PRELOAD
}
