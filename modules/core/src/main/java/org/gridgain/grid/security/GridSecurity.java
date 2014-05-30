/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.authentication.*;

import java.util.*;

/**
 * Grid security facade. This facade contains information about authenticated subjects
 * currently logged in to grid together with their permission sets.
 * <p>
 * You can get an instance of security facade from {@link Grid#security()} method.
 * <h1 class="header">Grid Nodes vs Remote Clients</h1>
 * When security is enabled, both grid nodes and remote clients must be authenticated.
 * For grid nodes, authentication parameters are specified in grid configuration via
 * {@link GridConfiguration#getSecurityCredentialsProvider()} provider. Here is an example
 * of how a simple user name and password may be provided:
 * <pre class="brush: java">
 *     GridConfiguration cfg = new GridConfiguration();
 *
 *     GridSecurityCredentials creds = new GridSecurityCredentials("username", "password");
 *
 *     cfg.setSecurityCredentialsProvider(new GridSecurityCredentialsBasicProvider(creds));
 *
 *     Grid grid = GridGain.start(cfg);
 * </pre>
 * For remote Java client, configuration is provided in a similar way by specifying
 * {@code GridClientConfiguration.setSecurityCredentialsProvider(...)} property.
 * <p>
 * For remote C++ and .NET clients, security credentials are provided in configuration
 * as well in the form of {@code "username:password"} string.
 * <h1 class="header">Security Permissions</h1>
 * Security permissions for every connected client or node are specified in {@link GridAuthenticationSpi}
 * configuration. All permissions supported by GridGain are provided in {@link GridSecurityPermission} enum.
 * <p>
 * Different SPI implementations may use different mechanisms to authenticate users, but
 * all SPIs should usually (although not required) specify security permissions in the following JSON format:
 * <pre class="brush: text">
 * {
 *     {
 *         "cache":"partitioned",
 *         "permissions":["CACHE_PUT", "CACHE_REMOVE", "CACHE_GET"]
 *     },
 *     {
 *         "cache":"*",
 *         "permissions":["CACHE_GET"]
 *     },
 *     {
 *         "task":"org.mytasks.*",
 *         "permissions":["TASK_EXECUTE"]
 *     },
 *     "defaultAllow":"false"
 * }
 * </pre>
 * Refer to documentation of available authentication SPIs for more information.
 */
public interface GridSecurity {
    /**
     * Gets collection of authorized subjects.
     *
     * @return Collection of authorized subjects.
     */
    public Collection<GridSecuritySubject> authenticatedSubjects() throws GridException;

    /**
     * Gets security subject by subject type and subject ID.
     *
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @return Authorized security subject.
     */
    public GridSecuritySubject authenticatedSubject(GridSecuritySubjectType subjType, UUID subjId) throws GridException;
}
