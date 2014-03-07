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
public class GridDsiResponse implements Serializable {
    /** */
    private long id;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private long msgId;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private long transactionId;

    /**
     * @param id ID.
     */
    public GridDsiResponse(long id) {
        this.id = id;
    }

    /**
     * @param terminalId Terminal ID.
     * @return Cache key.
     */
    public Object getCacheKey(String terminalId){
        //return new GridCacheAffinityKey<String>("RESPONSE:" + id.toString(), terminalId);
        return new ResponseKey(id, terminalId);
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class ResponseKey implements Serializable {
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
        ResponseKey(Long key, String terminalId) {
            this.key = key;
            this.terminalId = terminalId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof ResponseKey && key.equals(((ResponseKey)obj).key);
        }
    }
}
