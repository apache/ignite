/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.securesession;

import org.apache.ignite.plugin.security.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Secure session SPI allows for session creation and validation, typically after authentication
 * has successfully happened. The main purpose of this SPI is to ensure that remote clients are
 * authenticated only once and upon successful authentication get issued a secure session token
 * to reuse for consequent requests (very much the same way like HTTP sessions work).
 * <p>
 * The default secure session SPI is {@link org.apache.ignite.spi.securesession.noop.NoopSecureSessionSpi}
 * which permits any request.
 * <p>
 * Ignite provides the following {@code GridSecureSessionSpi} implementations:
 * <ul>
 * <li>
 *     {@link org.apache.ignite.spi.securesession.noop.NoopSecureSessionSpi} - permits any request.
 * </li>
 * <li>
 *     {@code GridRememberMeSecureSessionSpi} -
 *     validates client session with remember-me session token.
 * </li>
 * </ul>
 * <p>
 * <b>NOTE:</b> that multiple secure session SPIs may be started on the same grid node. In this case
 * Ignite will differentiate between them based on {@link #supported(GridSecuritySubjectType)}
 * value. The first SPI which returns {@code true} for a given subject type will be used for
 * session validation.
 * <p>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface SecureSessionSpi extends IgniteSpi {
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
     * @throws IgniteSpiException If validation resulted in system error. Note that
     *      bad credentials should not cause this exception.
     */
    public boolean validate(GridSecuritySubjectType subjType, UUID subjId, byte[] tok,
        @Nullable Object params) throws IgniteSpiException;

    /**
     * Generates new session token.
     *
     * @param subjType Subject type.
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param params Additional implementation-specific parameters.
     * @return Session token that should be used for further validation.
     */
    public byte[] generateSessionToken(GridSecuritySubjectType subjType, UUID subjId, @Nullable Object params)
        throws IgniteSpiException;
}
