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

package org.apache.ignite.internal.managers.communication;

/**
 * This enumeration defines different types of communication
 * message processing by the communication manager.
 */
public class GridIoPolicy {
    /** Public execution pool. */
    public static final byte PUBLIC_POOL = 0;

    /** P2P execution pool. */
    public static final byte P2P_POOL = 1;

    /** System execution pool. */
    public static final byte SYSTEM_POOL = 2;

    /** Management execution pool. */
    public static final byte MANAGEMENT_POOL = 3;

    /** Affinity fetch pool. */
    public static final byte AFFINITY_POOL = 4;

    /** Utility cache execution pool. */
    public static final byte UTILITY_CACHE_POOL = 5;

    /** Marshaller cache execution pool. */
    public static final byte MARSH_CACHE_POOL = 6;

    /**
     * Defines the range of reserved pools that are not available for plugins.
     * @param key The key.
     * @return If the key corresponds to reserved pool range.
     */
    public static boolean isReservedGridIoPolicy(byte key) {
        return key >= 0 && key <= 31;
    }
}