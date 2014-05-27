/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.security.os;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation for {@link GridSecurityManager}.
 */
public class GridOsSecurityManager extends GridNoopManagerAdapter implements GridSecurityManager {
    /**
     * @param ctx Kernal context.
     */
    public GridOsSecurityManager(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean securityEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridSecurityContext authenticateNode(GridNode node, GridSecurityCredentials cred)
        throws GridException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridSecurityContext authenticate(GridAuthenticationContext ctx) throws GridException {
        return Boolean.TRUE;
    }
}
