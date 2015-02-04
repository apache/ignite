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
 * environment, possibly in encrypted format. Ignite comes with
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
