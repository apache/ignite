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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.internal.processors.security.permission.IgnitePermission;

public final class SecurityConstants {

    public static final IgnitePermission JOIN_AS_SERVER_PERMISSION = new IgnitePermission("joinAsSever");

    public static final IgnitePermission EVENTS_ENABLE_PERMISSION = new IgnitePermission("eventsEnable");
    public static final IgnitePermission EVENTS_DISABLE_PERMISSION = new IgnitePermission("eventsDisable");

    public static final IgnitePermission VISOR_ADMIN_VIEW_PERMISSION = new IgnitePermission("visor.adminView");
    public static final IgnitePermission VISOR_ADMIN_QUERY_PERMISSION = new IgnitePermission("visor.adminQuery");
    public static final IgnitePermission VISOR_ADMIN_CACHE_PERMISSION = new IgnitePermission("visor.adminCache");
    public static final IgnitePermission VISOR_ADMIN_OPS_PERMISSION = new IgnitePermission("visor.adminOps");

    private SecurityConstants() {
        // No-op
    }
}
