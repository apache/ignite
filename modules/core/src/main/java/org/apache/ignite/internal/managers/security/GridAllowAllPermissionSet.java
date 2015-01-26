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

package org.apache.ignite.internal.managers.security;

import org.apache.ignite.plugin.security.*;

import java.util.*;

/**
* Allow all permission set.
*/
public class GridAllowAllPermissionSet implements GridSecurityPermissionSet {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean defaultAllowAll() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<GridSecurityPermission>> taskPermissions() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<GridSecurityPermission>> cachePermissions() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridSecurityPermission> systemPermissions() {
        return null;
    }

    /** {@inheritDoc} */
    public String toString() {
        return getClass().getSimpleName();
    }
}
