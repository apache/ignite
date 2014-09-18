/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.jetbrains.annotations.*;

import java.util.*;

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

    /** Get several cached values. */
    CACHE_GET_ALL("getall"),

    /** Store value in cache. */
    CACHE_PUT("put"),

    /** Store value in cache if it doesn't exist. */
    CACHE_ADD("add"),

    /** Store several values in cache. */
    CACHE_PUT_ALL("putall"),

    /** Remove value from cache. */
    CACHE_REMOVE("rmv"),

    /** Remove several values from cache. */
    CACHE_REMOVE_ALL("rmvall"),

    /** Replace cache value only if there is currently a mapping for it. */
    CACHE_REPLACE("rep"),

    /** Increment. */
    CACHE_INCREMENT("incr"),

    /** Decrement. */
    CACHE_DECREMENT("decr"),

    /** Compare and set. */
    CACHE_CAS("cas"),

    /** Append. */
    CACHE_APPEND("append"),

    /** Prepend. */
    CACHE_PREPEND("prepend"),

    /** Cache metrics. */
    CACHE_METRICS("cache"),

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

    /** Log. */
    LOG("log"),

    /** No-op. */
    NOOP("noop"),

    /** Quit. */
    QUIT("quit"),

    /** Start query execution. */
    CACHE_QUERY_EXECUTE("queryexecute"),

    /** Fetch query results. */
    CACHE_QUERY_FETCH("queryfetch"),

    /** Rebuild indexes. */
    CACHE_QUERY_REBUILD_INDEXES("rebuildqueryindexes"),

    /** Put portable metadata. */
    PUT_PORTABLE_METADATA("putportablemetadata"),

    /** Get portable metadata. */
    GET_PORTABLE_METADATA("getportablemetadata");

    /** Enum values. */
    private static final GridRestCommand[] VALS = values();

    /** Key to enum map. */
    private static final Map<String, GridRestCommand> cmds = new HashMap<>();

    /**
     * Map keys to commands.
     */
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
