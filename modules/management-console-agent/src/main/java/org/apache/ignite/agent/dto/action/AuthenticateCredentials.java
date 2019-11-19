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

package org.apache.ignite.agent.dto.action;

import java.net.InetSocketAddress;
import org.apache.ignite.plugin.security.SecurityCredentials;

/**
 * DTO for authenticate credentials.
 */
public class AuthenticateCredentials {
    /** Credentials. */
    private SecurityCredentials creds;

    /** Address. */
    private InetSocketAddress addr;

    /**
     * @return Security credentials.
     */
    public SecurityCredentials getCredentials() {
        return creds;
    }

    /**
     * @param creds Credentials.
     * @return {@code This} for chaining method calls.
     */
    public AuthenticateCredentials setCredentials(SecurityCredentials creds) {
        this.creds = creds;

        return this;
    }

    /**
     * @return Client inet socket address.
     */
    public InetSocketAddress getAddress() {
        return addr;
    }

    /**
     * @param addr Address.
     * @return {@code This} for chaining method calls.
     */
    public AuthenticateCredentials setAddress(InetSocketAddress addr) {
        this.addr = addr;

        return this;
    }
}
