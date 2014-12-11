/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.security;

import org.apache.ignite.*;

import java.util.*;

/**
 * Grid security facade. This facade contains information about authenticated subjects
 * currently logged in to grid together with their permission sets.
 * <p>
 * You can get an instance of security facade from {@link org.apache.ignite.Ignite#security()} method.
 * <h1 class="header">Grid Nodes vs Remote Clients</h1>
 * When security is enabled, both grid nodes and remote clients must be authenticated.
 * For grid nodes, authentication parameters are specified in grid configuration via
 * {@link org.apache.ignite.configuration.IgniteConfiguration#getSecurityCredentialsProvider()} provider. Here is an example
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
 * <h1 class="header">Authentication And Authorization</h1>
 * Node or client authentication happens in {@link org.apache.ignite.spi.authentication.AuthenticationSpi}. Upon successful
 * authentication, the SPI will return list of permissions for authenticated subject.
 * <p>
 * GridGain ships with following authentication SPIs out of the box:
 * <ul>
 * <li>{@code GridJaasAuthenticationSpi} - provides authentication based on JAAS standard.</li>
 * <li>{@code GridPasscodeAuthenticationSpi} - basic username and password authentication.</li>
 * </ul>
 * All permissions supported by GridGain are provided in {@link GridSecurityPermission} enum. Permissions
 * are specified on per-cache or per-task level (wildcards are allowed). Authentication SPIs should usually
 * (although not required) specify security permissions in the following JSON format:
 * <pre class="brush: text">
 * {
 *     {
 *         "cache":"partitioned",
 *         "permissions":["CACHE_PUT", "CACHE_REMOVE", "CACHE_READ"]
 *     },
 *     {
 *         "cache":"*",
 *         "permissions":["CACHE_READ"]
 *     },
 *     {
 *         "task":"org.mytasks.*",
 *         "permissions":["TASK_EXECUTE"]
 *     },
 *     {
 *         "system":["EVENTS_ENABLE", "ADMIN_VIEW"]
 *     }
 *     "defaultAllow":"false"
 * }
 * </pre>
 * Refer to documentation of available authentication SPIs for more information.
 */
public interface GridSecurity {
    /**
     * Gets collection of authenticated subjects together with their permissions.
     *
     * @return Collection of authenticated subjects.
     */
    public Collection<GridSecuritySubject> authenticatedSubjects() throws IgniteCheckedException;

    /**
     * Gets security subject based on subject ID.
     *
     * @param subjId Subject ID.
     * @return Authorized security subject.
     */
    public GridSecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException;
}
