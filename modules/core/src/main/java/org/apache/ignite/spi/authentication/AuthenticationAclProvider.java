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

package org.apache.ignite.spi.authentication;

import org.apache.ignite.*;
import org.apache.ignite.plugin.security.*;

import java.util.*;

/**
 * Access control list provider. Specific SPI implementation may use this
 * interface for declarative user permission specifications.
 * <p>
 * Abstracting access control specification through a provider allows users
 * to implement custom stores for per-user access control specifications,
 * e.g. encrypting them or storing in a separate file.
 */
public interface AuthenticationAclProvider {
    /**
     * Gets per-user access control map.
     *
     * @return Per-user access control map.
     * @throws IgniteException If failed.
     */
    public Map<GridSecurityCredentials, GridSecurityPermissionSet> acl() throws IgniteException;
}
