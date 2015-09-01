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

package org.apache.ignite.loadtests.dsi;

import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

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
        @AffinityKeyMapped
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