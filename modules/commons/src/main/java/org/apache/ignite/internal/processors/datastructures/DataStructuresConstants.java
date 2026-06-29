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

package org.apache.ignite.internal.processors.datastructures;

/**
 * Data structures constants shared between ignite-core and the thin client implementation.
 */
public final class DataStructuresConstants {
    /** Default data structures group name. */
    public static final String DEFAULT_DS_GROUP_NAME = "default-ds-group";

    /** Atomics system cache name. */
    public static final String ATOMICS_CACHE_NAME = "ignite-sys-atomic-cache";

    /** */
    private DataStructuresConstants() {
        // No-op.
    }
}
