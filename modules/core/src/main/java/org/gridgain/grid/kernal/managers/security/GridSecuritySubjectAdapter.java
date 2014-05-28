/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.security;

import org.gridgain.grid.security.*;

import java.net.*;
import java.util.*;

/**
 * Authenticated security subject.
 *
 * Example JSON configuration:
 * <code>
 *     {
 *         {
 *             "cache": "partitioned",
 *             "permissions": ["CACHE_PUT", "CACHE_READ"]
 *         },
 *         {
 *             "cache": "replicated",
 *             "permissions": ["CACHE_PUT", "CACHE_REMOVE"]
 *         },
 *         {
 *             "task": "org.mytask.*",
 *             "permissions": ["TASK_EXECUTE", "TASK_CANCEL"]
 *         }
 *     }
 * </code>
 */
public class GridSecuritySubjectAdapter implements GridSecuritySubject {
    /** Subject ID. */
    private UUID id;

    /** Subject type. */
    private GridSecuritySubjectType subjType;

    /** Address. */
    private InetAddress addr;

    /** Port. */
    private int port;

    /** Permissions assigned to a subject. */
    private GridSecurityPermissionSet permissions;

    /**
     * @param subjType Subject type.
     * @param id Subject ID.
     */
    public GridSecuritySubjectAdapter(GridSecuritySubjectType subjType, UUID id) {
        this.subjType = subjType;
        this.id = id;
    }

    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    @Override public UUID id() {
        return id;
    }

    /**
     * Gets subject type.
     *
     * @return Subject type.
     */
    @Override public GridSecuritySubjectType type() {
        return subjType;
    }

    /**
     * Gets subject address.
     *
     * @return Subject address.
     */
    @Override public InetAddress address() {
        return addr;
    }

    /**
     * Sets subject address.
     *
     * @param addr Subject address.
     */
    public void address(InetAddress addr) {
        this.addr = addr;
    }

    /**
     * Gets subject port.
     *
     * @return Subject port.
     */
    @Override public int port() {
        return port;
    }

    /**
     * Sets subject port.
     *
     * @param port Subject port.
     */
    public void port(int port) {
        this.port = port;
    }

    /**
     * Gets security permissions.
     *
     * @return Security permissions.
     */
    @Override public GridSecurityPermissionSet permissions() {
        return permissions;
    }

    /**
     * Sets security permissions.
     *
     * @param permissions Permissions.
     */
    public void permissions(GridSecurityPermissionSet permissions) {
        this.permissions = permissions;
    }
}
