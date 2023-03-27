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

import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 * Represents an interface that must be implemented by any internal {@link ComputeJob} that should be accessible for a
 * user to be executed through the Ignite public API.
 */
public interface PublicAccessJob {
    /** @return Set of permissions to be authorized before job execution. */
    public SecurityPermissionSet requiredPermissions();
}
