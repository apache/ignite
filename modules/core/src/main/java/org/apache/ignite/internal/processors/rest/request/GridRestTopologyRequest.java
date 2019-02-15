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

package org.apache.ignite.internal.processors.rest.request;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Grid command topology request.
 */
public class GridRestTopologyRequest extends GridRestRequest {
    /** Id of requested node. */
    private UUID nodeId;

    /** IP address of requested node. */
    private String nodeIp;

    /** Include metrics flag. */
    private boolean includeMetrics;

    /** Include node attributes flag. */
    private boolean includeAttrs;

    /** Include caches flag. With default value for compatibility. */
    private boolean includeCaches = true;

    /**
     * @return Include metrics flag.
     */
    public boolean includeMetrics() {
        return includeMetrics;
    }

    /**
     * @param includeMetrics Include metrics flag.
     */
    public void includeMetrics(boolean includeMetrics) {
        this.includeMetrics = includeMetrics;
    }

    /**
     * @return Include node attributes flag.
     */
    public boolean includeAttributes() {
        return includeAttrs;
    }

    /**
     * @param includeAttrs Include node attributes flag.
     */
    public void includeAttributes(boolean includeAttrs) {
        this.includeAttrs = includeAttrs;
    }

    /**
     * @return Include caches flag.
     */
    public boolean includeCaches() {
        return includeCaches;
    }

    /**
     * @param includeCaches Include caches flag.
     */
    public void includeCaches(boolean includeCaches) {
        this.includeCaches = includeCaches;
    }

    /**
     * @return Node identifier, if specified, {@code null} otherwise.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node identifier to lookup.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Node ip address if specified, {@code null} otherwise.
     */
    public String nodeIp() {
        return nodeIp;
    }

    /**
     * @param nodeIp Node ip address to lookup.
     */
    public void nodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestTopologyRequest.class, this, super.toString());
    }
}
