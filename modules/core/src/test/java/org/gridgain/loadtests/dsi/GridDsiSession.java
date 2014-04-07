/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.dsi;

import org.gridgain.grid.cache.affinity.*;

import java.io.*;

/**
 *
 */
public class GridDsiSession implements Serializable{
    /** */
    private String terminalId;

    /**
     * @param terminalId Terminal ID.
     */
    GridDsiSession(String terminalId) {
        this.terminalId = terminalId;
    }

    /**
     * @return Cache key.
     */
    public Object getCacheKey() {
        return getCacheKey(terminalId);
    }

    /**
     * @param terminalId Terminal ID.
     * @return Object.
     */
    public static Object getCacheKey(String terminalId) {
        return new SessionKey(terminalId + "SESSION", terminalId);
    }

    /**
     *
     */
    private static class SessionKey implements Serializable {
        /** */
        private String key;

        /** */
        @SuppressWarnings("UnusedDeclaration")
        @GridCacheAffinityKeyMapped
        private String terminalId;

        /**
         * @param key Key.
         * @param terminalId Terminal ID.
         */
        SessionKey(String key, String terminalId) {
            this.key = key;
            this.terminalId = terminalId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SessionKey && key.equals(((SessionKey)obj).key);
        }
    }
}
