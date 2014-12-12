/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.security;

import org.apache.ignite.*;

/**
 * Security credentials provider for specifying security credentials.
 * Security credentials used for client or node authentication.
 * <p>
 * For grid node, security credentials provider is specified in
 * {@link org.apache.ignite.configuration.IgniteConfiguration#setSecurityCredentialsProvider(GridSecurityCredentialsProvider)}
 * configuration property. For Java clients, you can provide credentials in
 * {@code GridClientConfiguration.setSecurityCredentialsProvider(...)} method.
 * <p>
 * Getting credentials through {@link GridSecurityCredentialsProvider} abstraction allows
 * users to provide custom implementations for storing user names and passwords in their
 * environment, possibly in encrypted format. GridGain comes with
 * {@link GridSecurityCredentialsBasicProvider} which simply provides
 * the passed in {@code login} and {@code password} when encryption or custom logic is not required.
 * <p>
 * In addition to {@code login} and {@code password}, security credentials allow for
 * specifying {@link GridSecurityCredentials#setUserObject(Object) userObject} as well, which can be used
 * to pass in any additional information required for authentication.
 */
public interface GridSecurityCredentialsProvider {
    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     * @throws IgniteCheckedException If failed.
     */
    public GridSecurityCredentials credentials() throws IgniteCheckedException;
}
