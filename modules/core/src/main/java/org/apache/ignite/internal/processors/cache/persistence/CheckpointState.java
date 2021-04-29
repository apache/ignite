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

package org.apache.ignite.internal.processors.cache.persistence;

/**
 * Possible checkpoint states. Ordinal is important. Every next state follows the previous one.
 */
public enum CheckpointState {
    /** Checkpoint is waiting to execution. **/
    SCHEDULED,

    /** Checkpoint was awakened and it is preparing to start. **/
    LOCK_TAKEN,

    /** Dirty pages snapshot has been taken. **/
    PAGE_SNAPSHOT_TAKEN,

    /** Checkpoint counted the pages and write lock was released. **/
    LOCK_RELEASED,

    /** Checkpoint marker was stored to disk. **/
    MARKER_STORED_TO_DISK,

    /** Checkpoint was finished. **/
    FINISHED
}
