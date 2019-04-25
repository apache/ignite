/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.plugin.security;

import org.apache.ignite.IgniteCheckedException;

/**
 * Basic implementation for {@link SecurityCredentialsProvider}. Use it
 * when custom logic for storing security credentials is not required and it
 * is OK to specify credentials directly in configuration.
 */
public class SecurityCredentialsBasicProvider implements SecurityCredentialsProvider {
    /** */
    private SecurityCredentials cred;

    /**
     * Constructs security credentials provider based on security credentials passed in.
     *
     * @param cred Security credentials.
     */
    public SecurityCredentialsBasicProvider(SecurityCredentials cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public SecurityCredentials credentials() throws IgniteCheckedException {
        return cred;
    }
}