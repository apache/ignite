/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.ignite.internal.processors.security;

import java.util.UUID;
import java.net.InetSocketAddress;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;

/**
 * Test security subject.
 */
public class TestSecuritySubject implements SecuritySubject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Login. */
    private String login;

    /** Permissions set. */
    private SecurityPermissionSet permsSet;

    /**
     * @param login Login.
     * @param permsSet Permissions set.
     */
    public TestSecuritySubject(String login, SecurityPermissionSet permsSet) {
        this.login = login;
        this.permsSet = permsSet;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return UUID.fromString("test uuid");
    }

    /** {@inheritDoc} */
    @Override public SecuritySubjectType type() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object login() {
        return login;
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress address() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecurityPermissionSet permissions() {
        return permsSet;
    }
}