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

package org.apache.ignite.failure;

/**
 * Types of failures.
 */
public enum FailureType {
    /** Node segmentation. */
    SEGMENTATION,

    /** System worker termination. */
    SYSTEM_WORKER_TERMINATION,

    /** System worker has not updated its heartbeat for a long time. */
    SYSTEM_WORKER_BLOCKED,

    /** Critical error - error which leads to the system's inoperability. */
    CRITICAL_ERROR,

    /** System-critical operation has been timed out. */
    SYSTEM_CRITICAL_OPERATION_TIMEOUT
}
