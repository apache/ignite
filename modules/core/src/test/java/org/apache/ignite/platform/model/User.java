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

package org.apache.ignite.platform.model;

import java.util.Objects;

/** Test value object. */
public class User {
    /** */
    private int id;

    /** */
    private ACL acl;

    /** */
    private Role role;

    /** */
    public User(int id, ACL acl, Role role) {
        this.id = id;
        this.acl = acl;
        this.role = role;
    }

    /** */
    public int getId() {
        return id;
    }

    /** */
    public void setId(int id) {
        this.id = id;
    }

    /** */
    public ACL getAcl() {
        return acl;
    }

    /** */
    public void setAcl(ACL acl) {
        this.acl = acl;
    }

    /** */
    public Role getRole() {
        return role;
    }

    /** */
    public void setRole(Role role) {
        this.role = role;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User)o;
        return id == user.id && acl == user.acl && Objects.equals(role, user.role);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id, acl, role);
    }
}
