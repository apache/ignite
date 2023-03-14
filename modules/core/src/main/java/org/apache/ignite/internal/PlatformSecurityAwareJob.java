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

package org.apache.ignite.internal;

import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Represents the base interface for all Platform Compute Jobs that wrap and execute user code. The execution and
 * cancellation of tasks marked with this interface will be preceded by authorization with the specified name and
 * {@link SecurityPermission#TASK_EXECUTE} permission.
 */
public interface PlatformSecurityAwareJob {
    /**
     * @return The name of the Platform Compute Job that will be used when authorizing its launch and cancellation.
     */
    public String name();
}
