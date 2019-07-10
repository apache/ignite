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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

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
     * Map of service names to service permissions. Wildcards are allowed at the
     * end of service names.
     *
     * @return Map of service names to service permissions.
     */
    public Map<String, Collection<SecurityPermission>> servicePermissions();

    /**
     * Collection of system-wide permissions (events enable/disable, Visor task execution).
     *
     * @return Collection of system-wide permissions ({@code null} if none).
     */
    public Collection<SecurityPermission> systemPermissions();
}
