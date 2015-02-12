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

package org.apache.ignite.authentication;

import org.apache.ignite.plugin.security.*;

import java.net.*;
import java.util.*;

/**
 * Authentication context.
 */
public class AuthenticationContextAdapter implements AuthenticationContext {
    /** Subject type. */
    private GridSecuritySubjectType subjType;

    /** Subject ID.w */
    private UUID subjId;

    /** Credentials. */
    private GridSecurityCredentials credentials;

    /** Subject address. */
    private InetSocketAddress addr;

    /**
     * Gets subject type.
     *
     * @return Subject type.
     */
    @Override public GridSecuritySubjectType subjectType() {
        return subjType;
    }

    /**
     * Sets subject type.
     *
     * @param subjType Subject type.
     */
    public void subjectType(GridSecuritySubjectType subjType) {
        this.subjType = subjType;
    }

    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    @Override public UUID subjectId() {
        return subjId;
    }

    /**
     * Sets subject ID.
     *
     * @param subjId Subject ID.
     */
    public void subjectId(UUID subjId) {
        this.subjId = subjId;
    }

    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     */
    @Override public GridSecurityCredentials credentials() {
        return credentials;
    }

    /**
     * Sets security credentials.
     *
     * @param credentials Security credentials.
     */
    public void credentials(GridSecurityCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Gets subject network address.
     *
     * @return Subject network address.
     */
    @Override public InetSocketAddress address() {
        return addr;
    }

    /**
     * Sets subject network address.
     *
     * @param addr Subject network address.
     */
    public void address(InetSocketAddress addr) {
        this.addr = addr;
    }
}
