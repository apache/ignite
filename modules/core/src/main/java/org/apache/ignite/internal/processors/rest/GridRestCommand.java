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
import java.util.function.Function;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;
import org.apache.ignite.plugin.security.SecurityPermission;
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
    CACHE_GET("get", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Contains cached value. */
    CACHE_CONTAINS_KEY("conkey", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Contains cached values. */
    CACHE_CONTAINS_KEYS("conkeys", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Get several cached values. */
    CACHE_GET_ALL("getall", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Store value in cache and return previous value. */
    CACHE_GET_AND_PUT("getput", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Store value in cache and return previous value. */
    CACHE_GET_AND_PUT_IF_ABSENT("getputifabs", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Store value in cache. */
    CACHE_PUT("put", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Store value in cache. */
    CACHE_PUT_IF_ABSENT("putifabs", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Store value in cache if it doesn't exist. */
    CACHE_ADD("add", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Store several values in cache. */
    CACHE_PUT_ALL("putall", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Remove value from cache. */
    CACHE_REMOVE("rmv", getCacheExtractor(), SecurityPermission.CACHE_REMOVE),

    /** Remove value from cache. */
    CACHE_REMOVE_VALUE("rmvval", getCacheExtractor(), SecurityPermission.CACHE_REMOVE),

    /** Remove value from cache. */
    CACHE_GET_AND_REMOVE("getrmv", getCacheExtractor(), SecurityPermission.CACHE_REMOVE),

    /** Remove several values from cache. */
    CACHE_REMOVE_ALL("rmvall", getCacheExtractor(), SecurityPermission.CACHE_REMOVE),

    /** Clear the specified cache, or all caches if none is specified. */
    CACHE_CLEAR("clear", getCacheExtractor(), SecurityPermission.CACHE_REMOVE),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_REPLACE("rep", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_REPLACE_VALUE("repval", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_GET_AND_REPLACE("getrep", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Compare and set. */
    CACHE_CAS("cas", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Append. */
    CACHE_APPEND("append", getCacheExtractor(), SecurityPermission.CACHE_PUT),

    /** Prepend. */
    CACHE_PREPEND("prepend", getCacheExtractor(), SecurityPermission.CACHE_PUT),

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
    EXE("exe", getTaskExtractor(), SecurityPermission.TASK_EXECUTE),

    /** Task execution .*/
    RESULT("res", getTaskExtractor(), SecurityPermission.TASK_EXECUTE),

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
    GET_OR_CREATE_CACHE("getorcreate",
        getCacheExtractor(), SecurityPermission.ADMIN_CACHE, SecurityPermission.CACHE_CREATE),

    /** Stops dynamically started cache. */
    DESTROY_CACHE("destcache",
        getCacheExtractor(), SecurityPermission.ADMIN_CACHE, SecurityPermission.CACHE_DESTROY),

    /** Execute sql query. */
    EXECUTE_SQL_QUERY("qryexe", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Execute sql fields query. */
    EXECUTE_SQL_FIELDS_QUERY("qryfldexe", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Execute scan query. */
    EXECUTE_SCAN_QUERY("qryscanexe", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Fetch query results. */
    FETCH_SQL_QUERY("qryfetch", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** Close query. */
    CLOSE_SQL_QUERY("qrycls", getCacheExtractor(), SecurityPermission.CACHE_READ),

    /** @deprecated Use {@link #CLUSTER_ACTIVATE} instead. */
    @Deprecated
    CLUSTER_ACTIVE("active"),

    /** @deprecated Use {@link #CLUSTER_DEACTIVATE} instead. */
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

    /** Permissions. */
    private final SecurityPermission[] perms;

    /** Index of task name wrapped by VisorGatewayTask */
    private static final int WRAPPED_TASK_IDX = 1;

    /** Name extractor. */
    private final Function<GridRestRequest, String> nameExtractor;

    /**
     * @param key Key.
     */
    GridRestCommand(String key) {
        this(key, r -> null);
    }

    GridRestCommand(String key, SecurityPermission... perm) {
        this(key, r -> null, perm);
    }

    GridRestCommand(String key, Function<GridRestRequest, String> nameExtractor, SecurityPermission... perms) {
        this.key = key;
        this.nameExtractor = nameExtractor;
        this.perms = perms;
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

    public SecurityPermission[] permissions(){
        return perms.clone();
    }

    private static Function<GridRestRequest, String> getCacheExtractor(){
        return r -> ((GridRestCacheRequest)r).cacheName();
    }

    private static Function<GridRestRequest, String> getTaskExtractor(){
        return r -> {
            GridRestTaskRequest taskReq = (GridRestTaskRequest)r;

            String name = taskReq.taskName();

            // We should extract task name wrapped by VisorGatewayTask.
            if (VisorGatewayTask.class.getName().equals(name))
                name = (String)taskReq.params().get(WRAPPED_TASK_IDX);

            return name;
        };
    }
    public String name(GridRestRequest req) {
        return nameExtractor.apply(req);
    }
}
