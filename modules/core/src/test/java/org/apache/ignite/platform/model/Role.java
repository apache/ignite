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
public class Role {
    /** */
    String name;

    /** */
    AccessLevel accessLevel;

    /** */
    public Role(String name, AccessLevel accessLevel) {
        this.name = name;
        this.accessLevel = accessLevel;
    }

    /** */
    public String getName() {
        return name;
    }

    /** */
    public void setName(String name) {
        this.name = name;
    }

    /** */
    public AccessLevel getAccessLevel() {
        return accessLevel;
    }

    /** */
    public void setAccessLevel(AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Role role = (Role)o;
        return Objects.equals(name, role.name) && accessLevel == role.accessLevel;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, accessLevel);
    }
}
