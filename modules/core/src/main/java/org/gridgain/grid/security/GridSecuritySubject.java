/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Security subject representing authenticated node or client with a set of permissions.
 * List of authenticated subjects can be retrieved from {@link GridSecurity#authenticatedSubjects()} method.
 */
public interface GridSecuritySubject extends Serializable {
    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    public UUID id();

    /**
     * Gets subject type, either node or client.
     *
     * @return Subject type.
     */
    public GridSecuritySubjectType type();

    /**
     * Login provided via subject security credentials.
     *
     * @return Login object.
     */
    public Object login();

    /**
     * Gets subject connection address. Usually {@link InetSocketAddress} representing connection IP and port.
     *
     * @return Subject connection address.
     */
    public InetSocketAddress address();

    /**
     * Authorized permission set for the subject.
     *
     * @return Authorized permission set for the subject.
     */
    public GridSecurityPermissionSet permissions();
}
