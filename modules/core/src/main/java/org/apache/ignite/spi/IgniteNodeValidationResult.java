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

package org.apache.ignite.spi;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Result of joining node validation.
 */
public class IgniteNodeValidationResult {
    /** Offending node ID. */
    private final UUID nodeId;

    /** Error message to be logged locally. */
    private final String msg;

    /** Error message to be sent to joining node. */
    private final String sndMsg;

    /**
     * @param nodeId Offending node ID.
     * @param msg Message logged locally.
     * @param sndMsg Message sent to joining node.
     */
    public IgniteNodeValidationResult(UUID nodeId, String msg, String sndMsg) {
        this.nodeId = nodeId;
        this.msg = msg;
        this.sndMsg = sndMsg;
    }

    /**
     * @return Offending node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Message to be logged locally.
     */
    public String message() {
        return msg;
    }

    /**
     * @return Message to be sent to joining node.
     */
    public String sendMessage() {
        return sndMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteNodeValidationResult.class, this);
    }
}
