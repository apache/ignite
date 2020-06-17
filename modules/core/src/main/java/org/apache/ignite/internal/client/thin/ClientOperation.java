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

package org.apache.ignite.internal.client.thin;

import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** Operation codes. */
enum ClientOperation {
    /** Resource close. */RESOURCE_CLOSE(0),

    /** Cache get or create with name. */CACHE_GET_OR_CREATE_WITH_NAME(1052),
    /** Cache put. */CACHE_PUT(1001),
    /** Cache get. */CACHE_GET(1000),
    /** Cache create with configuration. */CACHE_CREATE_WITH_CONFIGURATION(1053),
    /** Cache get names. */CACHE_GET_NAMES(1050),
    /** Cache destroy. */CACHE_DESTROY(1056),
    /** Cache get or create with configuration. */CACHE_GET_OR_CREATE_WITH_CONFIGURATION(1054),
    /** Cache create with name. */CACHE_CREATE_WITH_NAME(1051),
    /** Cache contains key. */CACHE_CONTAINS_KEY(1011),
    /** Cache get configuration. */CACHE_GET_CONFIGURATION(1055),
    /** Get size. */CACHE_GET_SIZE(1020),
    /** Put all. */CACHE_PUT_ALL(1004),
    /** Get all. */CACHE_GET_ALL(1003),
    /** Cache replace if equals. */CACHE_REPLACE_IF_EQUALS(1010),
    /** Cache replace. */CACHE_REPLACE(1009),
    /** Cache remove key. */CACHE_REMOVE_KEY(1016),
    /** Cache remove if equals. */CACHE_REMOVE_IF_EQUALS(1017),
    /** Cache remove keys. */CACHE_REMOVE_KEYS(1018),
    /** Cache remove all. */CACHE_REMOVE_ALL(1019),
    /** Cache get and put. */CACHE_GET_AND_PUT(1005),
    /** Cache get and remove. */CACHE_GET_AND_REMOVE(1007),
    /** Cache get and replace. */CACHE_GET_AND_REPLACE(1006),
    /** Cache put if absent. */CACHE_PUT_IF_ABSENT(1002),
    /** Cache clear. */CACHE_CLEAR(1013),
    /** Cache partitions. */CACHE_PARTITIONS(1101),

    /** Query scan. */QUERY_SCAN(2000),
    /** Query scan cursor get page. */QUERY_SCAN_CURSOR_GET_PAGE(2001),
    /** Query sql. */QUERY_SQL(2002),
    /** Query sql cursor get page. */QUERY_SQL_CURSOR_GET_PAGE(2003),
    /** Query sql fields. */QUERY_SQL_FIELDS(2004),
    /** Query sql fields cursor get page. */QUERY_SQL_FIELDS_CURSOR_GET_PAGE(2005),

    /** Get binary type. */GET_BINARY_TYPE(3002),
    /** Register binary type name. */REGISTER_BINARY_TYPE_NAME(3001),
    /** Put binary type. */PUT_BINARY_TYPE(3003),
    /** Get binary type name. */GET_BINARY_TYPE_NAME(3000),

    /** Start new transaction. */TX_START(4000),
    /** End the transaction (commit or rollback). */TX_END(4001),

    /** Get cluster state. */CLUSTER_GET_STATE(5000),
    /** Change cluster state. */CLUSTER_CHANGE_STATE(5001),
    /** Get WAL state. */CLUSTER_GET_WAL_STATE(5003),
    /** Change WAL state. */CLUSTER_CHANGE_WAL_STATE(5002),
    /** Get nodes IDs by filter. */CLUSTER_GROUP_GET_NODE_IDS(5100),
    /** Get nodes info by IDs. */CLUSTER_GROUP_GET_NODE_INFO(5101),

    /** Execute compute task. */COMPUTE_TASK_EXECUTE(6000),
    /** Finished compute task notification. */COMPUTE_TASK_FINISHED(6001, true),

    /** Invoke service. */SERVICE_INVOKE(7000);

    /** Code. */
    private final int code;

    /** Is notification. */
    private final boolean notification;

    /** Constructor. */
    ClientOperation(int code) {
        this(code, false);
    }

    /** Constructor. */
    ClientOperation(int code, boolean notification) {
        this.code = code;
        this.notification = notification;
    }

    /**
     * @return Code.
     */
    public short code() {
        return (short)code;
    }

    /**
     * @return {@code True} if operation is notification.
     */
    public boolean isNotification() {
        return notification;
    }

    /** Enum mapping from code to values. */
    private static final Map<Short, ClientOperation> map;

    static {
        map = new HashMap<>();

        for (ClientOperation op : values())
            map.put(op.code(), op);
    }

    /**
     * @param code Code to convert to enum.
     * @return Enum.
     */
    @Nullable public static ClientOperation fromCode(short code) {
        return map.get(code);
    }
}
