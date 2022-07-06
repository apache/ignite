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
import org.apache.ignite.client.ClientOperationType;
import org.jetbrains.annotations.Nullable;

/** Operation codes. */
public enum ClientOperation {
    /** Resource close. */
    RESOURCE_CLOSE(0),

    /** Heartbeat. */
    HEARTBEAT(1),

    /** Get idle timeout. */
    GET_IDLE_TIMEOUT(2),

    /** Cache get or create with name. */
    CACHE_GET_OR_CREATE_WITH_NAME(1052),

    /** Cache put. */
    CACHE_PUT(1001),

    /** Cache get. */
    CACHE_GET(1000),

    /** Cache create with configuration. */
    CACHE_CREATE_WITH_CONFIGURATION(1053),

    /** Cache get names. */
    CACHE_GET_NAMES(1050),

    /** Cache destroy. */
    CACHE_DESTROY(1056),

    /** Cache get or create with configuration. */
    CACHE_GET_OR_CREATE_WITH_CONFIGURATION(1054),

    /** Cache create with name. */
    CACHE_CREATE_WITH_NAME(1051),

    /** Cache contains key. */
    CACHE_CONTAINS_KEY(1011),

    /** Cache contains keys. */
    CACHE_CONTAINS_KEYS(1012),

    /** Cache get configuration. */
    CACHE_GET_CONFIGURATION(1055),

    /** Get size. */
    CACHE_GET_SIZE(1020),

    /** Put all. */
    CACHE_PUT_ALL(1004),

    /** Get all. */
    CACHE_GET_ALL(1003),

    /** Cache replace if equals. */
    CACHE_REPLACE_IF_EQUALS(1010),

    /** Cache replace. */
    CACHE_REPLACE(1009),

    /** Cache remove key. */
    CACHE_REMOVE_KEY(1016),

    /** Cache remove if equals. */
    CACHE_REMOVE_IF_EQUALS(1017),

    /** Cache remove keys. */
    CACHE_REMOVE_KEYS(1018),

    /** Cache remove all. */
    CACHE_REMOVE_ALL(1019),

    /** Cache get and put. */
    CACHE_GET_AND_PUT(1005),

    /** Cache get and remove. */
    CACHE_GET_AND_REMOVE(1007),

    /** Cache get and replace. */
    CACHE_GET_AND_REPLACE(1006),

    /** Cache put if absent. */
    CACHE_PUT_IF_ABSENT(1002),

    /** Cache get and put if absent. */
    CACHE_GET_AND_PUT_IF_ABSENT(1008),

    /** Cache clear. */
    CACHE_CLEAR(1013),

    /** Cache clear key. */
    CACHE_CLEAR_KEY(1014),

    /** Cache clear keys. */
    CACHE_CLEAR_KEYS(1015),

    /** Cache partitions. */
    CACHE_PARTITIONS(1101),

    /** Query scan. */
    QUERY_SCAN(2000),

    /** Query scan cursor get page. */
    QUERY_SCAN_CURSOR_GET_PAGE(2001),

    /** Query sql. */
    QUERY_SQL(2002),

    /** Query sql cursor get page. */
    QUERY_SQL_CURSOR_GET_PAGE(2003),

    /** Query sql fields. */
    QUERY_SQL_FIELDS(2004),

    /** Query sql fields cursor get page. */
    QUERY_SQL_FIELDS_CURSOR_GET_PAGE(2005),

    /** Continuous query. */
    QUERY_CONTINUOUS(2006),

    /** Continuous query event. */
    QUERY_CONTINUOUS_EVENT(2007, ClientNotificationType.CONTINUOUS_QUERY_EVENT),

    /** Get binary type name. */
    GET_BINARY_TYPE_NAME(3000),

    /** Register binary type name. */
    REGISTER_BINARY_TYPE_NAME(3001),

    /** Get binary type. */
    GET_BINARY_TYPE(3002),

    /** Put binary type. */
    PUT_BINARY_TYPE(3003),

    /** Get binary configuration. */
    GET_BINARY_CONFIGURATION(3004),

    /** Start new transaction. */
    TX_START(4000),

    /** End the transaction (commit or rollback). */
    TX_END(4001),

    /** Get cluster state. */
    CLUSTER_GET_STATE(5000),

    /** Change cluster state. */
    CLUSTER_CHANGE_STATE(5001),

    /** Get WAL state. */
    CLUSTER_GET_WAL_STATE(5003),

    /** Change WAL state. */
    CLUSTER_CHANGE_WAL_STATE(5002),

    /** Get nodes IDs by filter. */
    CLUSTER_GROUP_GET_NODE_IDS(5100),

    /** Get nodes info by IDs. */
    CLUSTER_GROUP_GET_NODE_INFO(5101),

    /** Execute compute task. */
    COMPUTE_TASK_EXECUTE(6000),

    /** Finished compute task notification. */
    COMPUTE_TASK_FINISHED(6001, ClientNotificationType.COMPUTE_TASK_FINISHED),

    /** Invoke service. */
    SERVICE_INVOKE(7000),

    /** Get service descriptors. */
    SERVICE_GET_DESCRIPTORS(7001),

    /** Get service descriptors. */
    SERVICE_GET_DESCRIPTOR(7002),

    /** Get or create an AtomicLong by name. */
    ATOMIC_LONG_CREATE(9000),

    /** Remove an AtomicLong. */
    ATOMIC_LONG_REMOVE(9001),

    /** Check if AtomicLong exists. */
    ATOMIC_LONG_EXISTS(9002),

    /** AtomicLong.get. */
    ATOMIC_LONG_VALUE_GET(9003),

    /** AtomicLong.addAndGet (also covers incrementAndGet, getAndIncrement, getAndAdd, decrementAndGet, getAndDecrement).  */
    ATOMIC_LONG_VALUE_ADD_AND_GET(9004),

    /** AtomicLong.getAndSet. */
    ATOMIC_LONG_VALUE_GET_AND_SET(9005),

    /** AtomicLong.compareAndSet. */
    ATOMIC_LONG_VALUE_COMPARE_AND_SET(9006),

    /** AtomicLong.compareAndSetAndGet. */
    ATOMIC_LONG_VALUE_COMPARE_AND_SET_AND_GET(9007),

    /** Create an IgniteSet. */
    OP_SET_GET_OR_CREATE(9010),

    /** Remove an IgniteSet. */
    OP_SET_CLOSE(9011),

    /** Check if IgniteSet exists. */
    OP_SET_EXISTS(9012),

    /** IgniteSet.add. */
    OP_SET_VALUE_ADD(9013),

    /** IgniteSet.addAll. */
    OP_SET_VALUE_ADD_ALL(9014),

    /** IgniteSet.remove. */
    OP_SET_VALUE_REMOVE(9015),

    /** IgniteSet.removeAll. */
    OP_SET_VALUE_REMOVE_ALL(9016),

    /** IgniteSet.contains. */
    OP_SET_VALUE_CONTAINS(9017),

    /** IgniteSet.containsAll. */
    OP_SET_VALUE_CONTAINS_ALL(9018),

    /** IgniteSet.retainAll. */
    OP_SET_VALUE_RETAIN_ALL(9019),

    /** IgniteSet.size. */
    OP_SET_SIZE(9020),

    /** IgniteSet.clear. */
    OP_SET_CLEAR(9021),

    /** IgniteSet.iterator. */
    OP_SET_ITERATOR_START(9022),

    /** IgniteSet.iterator page. */
    OP_SET_ITERATOR_GET_PAGE(9023);

    /** Code. */
    private final int code;

    /** Type of notification. */
    private final ClientNotificationType notificationType;

    /** Constructor. */
    ClientOperation(int code) {
        this(code, null);
    }

    /** Constructor. */
    ClientOperation(int code, ClientNotificationType notificationType) {
        this.code = code;
        this.notificationType = notificationType;
    }

    /**
     * @return Code.
     */
    public short code() {
        return (short)code;
    }

    /**
     * @return Type of notification.
     */
    public ClientNotificationType notificationType() {
        return notificationType;
    }

    /**
     * Converts this internal operation code to public {@link ClientOperationType}.
     *
     * @return Corresponding {@link ClientOperationType}, or {@code null} if there is no match.
     * Some operations, such as {@link #RESOURCE_CLOSE}, do not have a public counterpart.
     */
    @Nullable public ClientOperationType toPublicOperationType() {
        switch (this) {
            case CACHE_GET_OR_CREATE_WITH_NAME:
            case CACHE_GET_OR_CREATE_WITH_CONFIGURATION:
                return ClientOperationType.CACHE_GET_OR_CREATE;

            case CACHE_CREATE_WITH_CONFIGURATION:
            case CACHE_CREATE_WITH_NAME:
                return ClientOperationType.CACHE_CREATE;

            case CACHE_PUT:
                return ClientOperationType.CACHE_PUT;

            case CACHE_GET:
                return ClientOperationType.CACHE_GET;

            case CACHE_GET_NAMES:
                return ClientOperationType.CACHE_GET_NAMES;

            case CACHE_DESTROY:
                return ClientOperationType.CACHE_DESTROY;

            case CACHE_CONTAINS_KEY:
                return ClientOperationType.CACHE_CONTAINS_KEY;

            case CACHE_CONTAINS_KEYS:
                return ClientOperationType.CACHE_CONTAINS_KEYS;

            case CACHE_GET_CONFIGURATION:
                return ClientOperationType.CACHE_GET_CONFIGURATION;

            case CACHE_GET_SIZE:
                return ClientOperationType.CACHE_GET_SIZE;

            case CACHE_PUT_ALL:
                return ClientOperationType.CACHE_PUT_ALL;

            case CACHE_GET_ALL:
                return ClientOperationType.CACHE_GET_ALL;

            case CACHE_REPLACE_IF_EQUALS:
            case CACHE_REPLACE:
                return ClientOperationType.CACHE_REPLACE;

            case CACHE_REMOVE_KEY:
            case CACHE_REMOVE_IF_EQUALS:
                return ClientOperationType.CACHE_REMOVE_ONE;

            case CACHE_REMOVE_KEYS:
                return ClientOperationType.CACHE_REMOVE_MULTIPLE;

            case CACHE_REMOVE_ALL:
                return ClientOperationType.CACHE_REMOVE_EVERYTHING;

            case CACHE_GET_AND_PUT:
                return ClientOperationType.CACHE_GET_AND_PUT;

            case CACHE_GET_AND_REMOVE:
                return ClientOperationType.CACHE_GET_AND_REMOVE;

            case CACHE_GET_AND_REPLACE:
                return ClientOperationType.CACHE_GET_AND_REPLACE;

            case CACHE_PUT_IF_ABSENT:
                return ClientOperationType.CACHE_PUT_IF_ABSENT;

            case CACHE_GET_AND_PUT_IF_ABSENT:
                return ClientOperationType.CACHE_GET_AND_PUT_IF_ABSENT;

            case CACHE_CLEAR:
                return ClientOperationType.CACHE_CLEAR_EVERYTHING;

            case CACHE_CLEAR_KEY:
                return ClientOperationType.CACHE_CLEAR_ONE;

            case CACHE_CLEAR_KEYS:
                return ClientOperationType.CACHE_CLEAR_MULTIPLE;

            case QUERY_SCAN:
                return ClientOperationType.QUERY_SCAN;

            case QUERY_SQL:
            case QUERY_SQL_FIELDS:
                return ClientOperationType.QUERY_SQL;

            case QUERY_CONTINUOUS:
                return ClientOperationType.QUERY_CONTINUOUS;

            case TX_START:
                return ClientOperationType.TRANSACTION_START;

            case CLUSTER_GET_STATE:
                return ClientOperationType.CLUSTER_GET_STATE;

            case CLUSTER_CHANGE_STATE:
                return ClientOperationType.CLUSTER_CHANGE_STATE;

            case CLUSTER_GET_WAL_STATE:
                return ClientOperationType.CLUSTER_GET_WAL_STATE;

            case CLUSTER_CHANGE_WAL_STATE:
                return ClientOperationType.CLUSTER_CHANGE_WAL_STATE;

            case CLUSTER_GROUP_GET_NODE_IDS:
            case CLUSTER_GROUP_GET_NODE_INFO:
                return ClientOperationType.CLUSTER_GROUP_GET_NODES;

            case COMPUTE_TASK_EXECUTE:
                return ClientOperationType.COMPUTE_TASK_EXECUTE;

            case SERVICE_INVOKE:
                return ClientOperationType.SERVICE_INVOKE;

            case SERVICE_GET_DESCRIPTORS:
                return ClientOperationType.SERVICE_GET_DESCRIPTORS;

            case SERVICE_GET_DESCRIPTOR:
                return ClientOperationType.SERVICE_GET_DESCRIPTOR;

            case ATOMIC_LONG_CREATE:
                return ClientOperationType.ATOMIC_LONG_CREATE;

            case ATOMIC_LONG_REMOVE:
                return ClientOperationType.ATOMIC_LONG_REMOVE;

            case ATOMIC_LONG_EXISTS:
                return ClientOperationType.ATOMIC_LONG_EXISTS;

            case ATOMIC_LONG_VALUE_GET:
                return ClientOperationType.ATOMIC_LONG_VALUE_GET;

            case ATOMIC_LONG_VALUE_ADD_AND_GET:
                return ClientOperationType.ATOMIC_LONG_VALUE_ADD_AND_GET;

            case ATOMIC_LONG_VALUE_GET_AND_SET:
                return ClientOperationType.ATOMIC_LONG_VALUE_GET_AND_SET;

            case ATOMIC_LONG_VALUE_COMPARE_AND_SET:
            case ATOMIC_LONG_VALUE_COMPARE_AND_SET_AND_GET:
                return ClientOperationType.ATOMIC_LONG_VALUE_COMPARE_AND_SET;

            case OP_SET_GET_OR_CREATE:
                return ClientOperationType.SET_GET_OR_CREATE;

            case OP_SET_CLOSE:
                return ClientOperationType.SET_REMOVE;

            case OP_SET_EXISTS:
                return ClientOperationType.SET_EXISTS;

            case OP_SET_VALUE_ADD:
                return ClientOperationType.SET_VALUE_ADD;

            case OP_SET_VALUE_ADD_ALL:
                return ClientOperationType.SET_VALUE_ADD_ALL;

            case OP_SET_VALUE_REMOVE:
                return ClientOperationType.SET_VALUE_REMOVE;

            case OP_SET_VALUE_REMOVE_ALL:
                return ClientOperationType.SET_VALUE_REMOVE_ALL;

            case OP_SET_VALUE_CONTAINS:
                return ClientOperationType.SET_VALUE_CONTAINS;

            case OP_SET_VALUE_CONTAINS_ALL:
                return ClientOperationType.SET_VALUE_CONTAINS_ALL;

            case OP_SET_VALUE_RETAIN_ALL:
                return ClientOperationType.SET_VALUE_RETAIN_ALL;

            case OP_SET_SIZE:
                return ClientOperationType.SET_SIZE;

            case OP_SET_CLEAR:
                return ClientOperationType.SET_CLEAR;

            case OP_SET_ITERATOR_START:
                return ClientOperationType.SET_ITERATOR;

            default:
                return null;
        }
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
