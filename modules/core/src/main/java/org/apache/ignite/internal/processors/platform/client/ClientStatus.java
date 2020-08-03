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

package org.apache.ignite.internal.processors.platform.client;

/**
 * Client status codes.
 */
public final class ClientStatus {
    /**
     * No-op constructor to prevent instantiation.
     */
    private ClientStatus() {
        // No-op.
    }

    /** Command succeeded. */
    public static final int SUCCESS = 0;

    /** Command failed. */
    public static final int FAILED = 1;

    /** Invalid op code. */
    public static final int INVALID_OP_CODE = 2;

    /** Functionality is disabled. */
    public static final int FUNCTIONALITY_DISABLED = 100;

    /** Cache does not exist. */
    public static final int CACHE_DOES_NOT_EXIST = 1000;

    /** Cache already exists. */
    public static final int CACHE_EXISTS = 1001;

    /** Too many cursors. */
    public static final int TOO_MANY_CURSORS = 1010;

    /** Resource does not exist. */
    public static final int RESOURCE_DOES_NOT_EXIST = 1011;

    /** Authorization failure. */
    public static final int SECURITY_VIOLATION = 1012;

    /** Active transactions per connection limit exceeded. */
    public static final int TX_LIMIT_EXCEEDED = 1020;

    /** Transaction not found. */
    public static final int TX_NOT_FOUND = 1021;

    /** Too many compute tasks. */
    public static final int TOO_MANY_COMPUTE_TASKS = 1030;

    /** Authentication failed. */
    public static final int AUTH_FAILED = 2000;
}
