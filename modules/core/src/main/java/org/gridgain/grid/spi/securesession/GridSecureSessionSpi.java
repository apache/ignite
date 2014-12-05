/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession;

import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.securesession.noop.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Secure session SPI allows for session creation and validation, typically after authentication
 * has successfully happened. The main purpose of this SPI is to ensure that remote clients are
 * authenticated only once and upon successful authentication get issued a secure session token
 * to reuse for consequent requests (very much the same way like HTTP sessions work).
 * <p>
 * The default secure session SPI is {@link GridNoopSecureSessionSpi}
 * which permits any request.
 * <p>
 * Gridgain provides the following {@code GridSecureSessionSpi} implementations:
 * <ul>
 * <li>
 *     {@link GridNoopSecureSessionSpi} - permits any request.
 * </li>
 * <li>
 *     {@code GridRememberMeSecureSessionSpi} -
 *     validates client session with remember-me session token.
 * </li>
 * </ul>
 * <p>
 * <b>NOTE:</b> that multiple secure session SPIs may be started on the same grid node. In this case
 * GridGain will differentiate between them based on {@link #supported(GridSecuritySubjectType)}
 * value. The first SPI which returns {@code true} for a given subject type will be used for
 * session validation.
 * <p>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface GridSecureSessionSpi extends IgniteSpi {
    /**
     * Checks if given subject is supported by this SPI. If not, then next secure session SPI
     * in the list will be checked.
     *
     * @param subjType Subject type.
     * @return {@code True} if subject type is supported, {@code false} otherwise.
     */
    public boolean supported(GridSecuritySubjectType subjType);

    /**
     * Validates given session token.
     *
     * @param subjType Subject type.
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param tok Token to validate.
     * @param params Additional implementation-specific parameters.
     * @return {@code True} if session token is valid, {@code false} otherwise.
     * @throws GridSpiException If validation resulted in system error. Note that
     *      bad credentials should not cause this exception.
     */
    public boolean validate(GridSecuritySubjectType subjType, UUID subjId, byte[] tok,
        @Nullable Object params) throws GridSpiException;

    /**
     * Generates new session token.
     *
     * @param subjType Subject type.
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param params Additional implementation-specific parameters.
     * @return Session token that should be used for further validation.
     */
    public byte[] generateSessionToken(GridSecuritySubjectType subjType, UUID subjId, @Nullable Object params)
        throws GridSpiException;
}
