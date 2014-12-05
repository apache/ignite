/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication;

import org.gridgain.grid.security.*;

import java.net.*;
import java.util.*;

/**
 * Authentication context.
 */
public interface AuthenticationContext {
    /**
     * Gets subject type.
     *
     * @return Subject type.
     */
    public GridSecuritySubjectType subjectType();

    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    public UUID subjectId();

    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     */
    public GridSecurityCredentials credentials();

    /**
     * Gets subject network address.
     *
     * @return Subject network address.
     */
    public InetSocketAddress address();
}
