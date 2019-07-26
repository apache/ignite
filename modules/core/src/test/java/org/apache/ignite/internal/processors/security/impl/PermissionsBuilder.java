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

package org.apache.ignite.internal.processors.security.impl;

import java.security.AllPermission;
import java.security.Permission;
import java.security.Permissions;

/** */
public class PermissionsBuilder {
    /** */
    public static PermissionsBuilder create() {
        return new PermissionsBuilder();
    }

    /** */
    public static Permissions createAllowAll() {
        Permissions res = new Permissions();

        res.add(new AllPermission());

        return res;
    }

    /** */
    private final Permissions perms;

    /** */
    private PermissionsBuilder() {
        perms = new Permissions();
    }

    /** */
    public PermissionsBuilder add(Permission perm) {
        perms.add(perm);

        return this;
    }

    /** */
    public Permissions get() {
        return perms;
    }
}
