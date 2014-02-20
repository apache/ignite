// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.authentication;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This interface defines a grid authentication manager.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridAuthenticationManager extends GridManager {
    /**
     * Checks if security check is enabled.
     *
     * @return {@code True} if authentication check is enabled.
     */
    public boolean securityEnabled();

    /**
     * Authenticates grid node with it's attributes via underlying {@link GridAuthenticationSpi}s.
     *
     * @param nodeId Node id to authenticate.
     * @param attrs Node attributes.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws GridException If error occurred.
     */
    public boolean authenticateNode(UUID nodeId, Map<String, Object> attrs) throws GridException;

    /**
     * Authenticates subject via underlying {@link GridAuthenticationSpi}s.
     *
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param creds Credentials.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws GridException If error occurred.
     */
    public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId,
        @Nullable Object creds) throws GridException;
}
