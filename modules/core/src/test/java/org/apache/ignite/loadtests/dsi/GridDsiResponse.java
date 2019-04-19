/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.loadtests.dsi;

import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 *
 */
public class GridDsiResponse implements Serializable {
    /** */
    private long id;

    /** */
    private long msgId;

    /** */
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
        //return new AffinityKey<String>("RESPONSE:" + id.toString(), terminalId);
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
        @AffinityKeyMapped
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