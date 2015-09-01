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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Security permission set for authorized security subjects. Permission set
 * allows to specify task permissions for every task and cache permissions
 * for every cache. While cards are supported at the end of task or
 * cache name.
 * <p>
 * Property {@link #defaultAllowAll()} specifies whether to allow or deny
 * cache and task operations if they were not explicitly specified.
 */
public interface SecurityPermissionSet extends Serializable {
    /**
     * Flag indicating whether to allow or deny cache and task operations
     * if they were not explicitly specified.
     *
     * @return {@code True} to allow all cache task operations if they were
     *      not explicitly specified, {@code false} otherwise.
     */
    public boolean defaultAllowAll();

    /**
     * Map of task names to task permissions. Wildcards are allowed at the
     * end of task names.
     *
     * @return Map of task names to task permissions.
     */
    public Map<String, Collection<SecurityPermission>> taskPermissions();

    /**
     * Map of cache names to cache permissions. Wildcards are allowed at the
     * end of cache names.
     *
     * @return Map of cache names to cache permissions.
     */
    public Map<String, Collection<SecurityPermission>> cachePermissions();

    /**
     * Collection of system-wide permissions (events enable/disable, Visor task execution).
     *
     * @return Collection of system-wide permissions.
     */
    @Nullable public Collection<SecurityPermission> systemPermissions();
}