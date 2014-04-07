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
public class GridDsiRequest implements Serializable {
    /** */
    private Long id;

    /** */
    @SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
    private long msgId;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private long txId;

    /**
     * @param id ID.
     */
    public GridDsiRequest(long id) {
        this.id = id;
    }

    /**
     * @param msgId Message ID.
     */
    public void setMessageId(long msgId) {
        this.msgId = msgId;
    }

    /**
     * @param terminalId Terminal ID.
     * @return Cache key.
     */
    public Object getCacheKey(String terminalId){
        return new RequestKey(id, terminalId);
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class RequestKey implements Serializable {
        /** */
        private Long key;

        /** */
        @SuppressWarnings("UnusedDeclaration")
        @GridCacheAffinityKeyMapped
        private String terminalId;

        /**
         * @param key Key.
         * @param terminalId Terminal ID.
         */
        RequestKey(long key, String terminalId) {
            this.key = key;
            this.terminalId = terminalId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof RequestKey && key.equals(((RequestKey)obj).key);
        }
    }
}
