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

package org.apache.ignite.internal.processors.rest;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cluster.ClusterState;
import org.jetbrains.annotations.Nullable;

/**
 * Supported commands.
 */
public enum GridRestCommand {
    /*
     * API commands.
     * =============
     */

    /** Get cached value. */
    CACHE_GET("get"),

    /** Contains cached value. */
    CACHE_CONTAINS_KEY("conkey"),

    /** Contains cached values. */
    CACHE_CONTAINS_KEYS("conkeys"),

    /** Get several cached values. */
    CACHE_GET_ALL("getall"),

    /** Store value in cache and return previous value. */
    CACHE_GET_AND_PUT("getput"),

    /** Store value in cache and return previous value. */
    CACHE_GET_AND_PUT_IF_ABSENT("getputifabs"),

    /** Store value in cache. */
    CACHE_PUT("put"),

    /** Store value in cache. */
    CACHE_PUT_IF_ABSENT("putifabs"),

    /** Store value in cache if it doesn't exist. */
    CACHE_ADD("add"),

    /** Store several values in cache. */
    CACHE_PUT_ALL("putall"),

    /** Remove value from cache. */
    CACHE_REMOVE("rmv"),

    /** Remove value from cache. */
    CACHE_REMOVE_VALUE("rmvval"),

    /** Remove value from cache. */
    CACHE_GET_AND_REMOVE("getrmv"),

    /** Remove several values from cache. */
    CACHE_REMOVE_ALL("rmvall"),

    /** Clear the specified cache, or all caches if none is specified. */
    CACHE_CLEAR("clear"),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_REPLACE("rep"),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_REPLACE_VALUE("repval"),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_GET_AND_REPLACE("getrep"),

    /** Compare and set. */
    CACHE_CAS("cas"),

    /** Append. */
    CACHE_APPEND("append"),

    /** Prepend. */
    CACHE_PREPEND("prepend"),

    /** Cache metrics. */
    CACHE_METRICS("cache"),

    /** Cache size. */
    CACHE_SIZE("size"),

    /** Set TTL for the key. */
    CACHE_UPDATE_TLL("updatettl"),

    /** Cache metadata. */
    CACHE_METADATA("metadata"),

    /** Increment. */
    ATOMIC_INCREMENT("incr"),

    /** Decrement. */
    ATOMIC_DECREMENT("decr"),

    /** Grid topology. */
    TOPOLOGY("top"),

    /** Single node info. */
    NODE("node"),

    /** Task execution .*/
    EXE("exe"),

    /** Task execution .*/
    RESULT("res"),

    /** Version. */
    VERSION("version"),

    /** Name. */
    NAME("name"),

    /** Log. */
    LOG("log"),

    /** No-op. */
    NOOP("noop"),

    /** Quit. */
    QUIT("quit"),

    /** Get or create cache. */
    GET_OR_CREATE_CACHE("getorcreate"),

    /** Stops dynamically started cache. */
    DESTROY_CACHE("destcache"),

    /** Execute sql query. */
    EXECUTE_SQL_QUERY("qryexe"),

    /** Execute sql fields query. */
    EXECUTE_SQL_FIELDS_QUERY("qryfldexe"),

    /** Execute scan query. */
    EXECUTE_SCAN_QUERY("qryscanexe"),

    /** Fetch query results. */
    FETCH_SQL_QUERY("qryfetch"),

    /** Close query. */
    CLOSE_SQL_QUERY("qrycls"),

    /** @deprecated Use {@link #CLUSTER_SET_STATE} with {@link ClusterState#ACTIVE} instead. */
    @Deprecated
    CLUSTER_ACTIVE("active"),

    /** @deprecated Use {@link #CLUSTER_SET_STATE} with {@link ClusterState#INACTIVE} instead. */
    @Deprecated
    CLUSTER_INACTIVE("inactive"),

    /** @deprecated Use {@link #CLUSTER_SET_STATE} with {@link ClusterState#ACTIVE} instead. */
    @Deprecated
    CLUSTER_ACTIVATE("activate"),

    /** @deprecated Use {@link #CLUSTER_SET_STATE} with {@link ClusterState#INACTIVE} instead. */
    @Deprecated
    CLUSTER_DEACTIVATE("deactivate"),

    /** @deprecated Use {@link #CLUSTER_STATE} instead. */
    @Deprecated
    CLUSTER_CURRENT_STATE("currentstate"),

    /** */
    CLUSTER_NAME("clustername"),

    /** */
    CLUSTER_STATE("state"),

    /** */
    CLUSTER_SET_STATE("setstate"),

    /** */
    BASELINE_CURRENT_STATE("baseline"),

    /** */
    BASELINE_SET("setbaseline"),

    /** */
    BASELINE_ADD("addbaseline"),

    /** */
    BASELINE_REMOVE("removebaseline"),

    /** */
    AUTHENTICATE("authenticate"),

    /** */
    ADD_USER("adduser"),

    /** */
    REMOVE_USER("removeuser"),

    /** */
    UPDATE_USER("updateuser"),

    /** Data region metrics. */
    DATA_REGION_METRICS("dataregion"),

    /** Data storage metrics. */
    DATA_STORAGE_METRICS("datastorage");

    /** Enum values. */
    private static final GridRestCommand[] VALS = values();

    /** Key to enum map. */
    private static final Map<String, GridRestCommand> cmds = new HashMap<>();

    // Map keys to commands.
    static {
        for (GridRestCommand cmd : values())
            cmds.put(cmd.key(), cmd);
    }

    /** Command key. */
    private final String key;

    /**
     * @param key Key.
     */
    GridRestCommand(String key) {
        this.key = key;
    }

    /**
     * @param ord Byte to convert to enum.
     * @return Enum.
     */
    @Nullable public static GridRestCommand fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * @param key Key.
     * @return Command.
     */
    @Nullable public static GridRestCommand fromKey(String key) {
        return cmds.get(key);
    }

    /**
     * @return Command key.
     */
    public String key() {
        return key;
    }
}
