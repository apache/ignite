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

package org.apache.ignite.internal.processor.security;

import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;

/**
 * Security subject for tests.
 */
public class TestSecuritySubject implements SecuritySubject {

    private UUID id;
    private SecuritySubjectType type = SecuritySubjectType.REMOTE_NODE;
    private Object login;
    private InetSocketAddress address;
    private SecurityPermissionSet permissions;

    public TestSecuritySubject() {
    }

    public TestSecuritySubject(UUID id,
        Object login,
        InetSocketAddress address,
        SecurityPermissionSet permissions) {
        this.id = id;
        this.login = login;
        this.address = address;
        this.permissions = permissions;
    }

    @Override public UUID id() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    @Override public SecuritySubjectType type() {
        return type;
    }

    public void setType(SecuritySubjectType type) {
        this.type = type;
    }

    @Override public Object login() {
        return login;
    }

    public void setLogin(Object login) {
        this.login = login;
    }

    @Override public InetSocketAddress address() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    @Override public SecurityPermissionSet permissions() {
        return permissions;
    }

    public void setPermissions(SecurityPermissionSet permissions) {
        this.permissions = permissions;
    }

    @Override public String toString() {
        return "TestSecuritySubject{" +
            "id=" + id +
            ", type=" + type +
            ", login=" + login +
            '}';
    }
}
