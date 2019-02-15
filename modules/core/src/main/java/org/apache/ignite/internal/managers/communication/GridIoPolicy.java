/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.managers.communication;

/**
 * This enumeration defines different types of communication
 * message processing by the communication manager.
 */
public class GridIoPolicy {
    /** */
    public static final byte UNDEFINED = -1;

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

    /** IGFS pool. */
    public static final byte IGFS_POOL = 6;

    /** Pool for handling distributed index range requests. */
    public static final byte IDX_POOL = 7;

    /** Data streamer execution pool. */
    public static final byte DATA_STREAMER_POOL = 9;

    /** Query execution pool. */
    public static final byte QUERY_POOL = 10;

    /** Pool for service proxy executions. */
    public static final byte SERVICE_POOL = 11;

    /** Schema pool.  */
    public static final byte SCHEMA_POOL = 12;

    /**
     * Defines the range of reserved pools that are not available for plugins.
     * @param key The key.
     * @return If the key corresponds to reserved pool range.
     */
    public static boolean isReservedGridIoPolicy(byte key) {
        return key >= 0 && key <= 31;
    }
}
