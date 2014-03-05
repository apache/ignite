/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.securesession;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

/**
 * This interface defines a grid secure session manager.
 */
public interface GridSecureSessionManager extends GridManager {
    /**
     * Checks if security check is enabled.
     *
     * @return {@code True} if secure session check is enabled.
     */
    public boolean securityEnabled();

    /**
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param tok Token.
     * @param params Parameters.
     * @return Next token.
     * @throws GridException If error occurred.
     */
    @Nullable public byte[] validate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridException;
}
