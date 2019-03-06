/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.loadtests.dsi;

import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

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
        @AffinityKeyMapped
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