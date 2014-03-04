/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication.jaas;

import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import javax.security.auth.callback.*;

/**
 * Callback handler factory for Jaas login module.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridJaasCallbackHandlerFactory {
    /**
     * Checks if given subject is supported by this factory.
     *
     * @param subjType Subject type.
     * @return {@code True} if subject type is supported, {@code false} otherwise.
     */
    boolean supported(GridSecuritySubjectType subjType);

    /**
     * Create new authentication callbacks handler.
     *
     * @param subjType Subject type.
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param creds Authentication parameters (may be {@code null} or empty based on implementation).
     * @return New callbacks handler which may be used to authenticate requestor via Jaas security scheme.
     *      Or null if such handler cannot be instantiated.
     * @throws GridSpiException If callback creation resulted in system error. Note that
     *      bad credentials should not cause this exception.
     */
    @Nullable CallbackHandler newInstance(GridSecuritySubjectType subjType, byte[] subjId,
        @Nullable Object creds) throws GridSpiException;
}
