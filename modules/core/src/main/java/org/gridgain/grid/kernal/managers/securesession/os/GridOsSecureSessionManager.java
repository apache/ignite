/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.securesession.os;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.kernal.managers.securesession.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation for {@link GridSecureSessionManager}.
 */
public class GridOsSecureSessionManager extends GridNoopManagerAdapter implements GridSecureSessionManager {
    /** Empty bytes. */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * @param ctx Kernal context.
     */
    public GridOsSecureSessionManager(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean securityEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean validate(GridSecuritySubjectType subjType, UUID subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte[] generateSessionToken(GridSecuritySubjectType subjType, UUID subjId,
        @Nullable Object params) {
        return EMPTY_BYTES;
    }
}
