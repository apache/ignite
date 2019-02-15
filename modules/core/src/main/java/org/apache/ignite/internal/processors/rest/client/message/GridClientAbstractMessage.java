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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class provides implementation for commit message fields and cannot be used directly.
 */
public abstract class GridClientAbstractMessage implements GridClientMessage, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request ID (transient). */
    private transient long reqId;

    /** Client ID (transient). */
    private transient UUID id;

    /** Node ID (transient). */
    private transient UUID destId;

    /** Session token. */
    private byte[] sesTok;

    /** {@inheritDoc} */
    @Override public long requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public UUID clientId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public void clientId(UUID id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public UUID destinationId() {
        return destId;
    }

    /** {@inheritDoc} */
    @Override public void destinationId(UUID destId) {
        this.destId = destId;
    }

    /**
     * @return Session token
     */
    @Override public byte[] sessionToken() {
        return sesTok;
    }

    /**
     * @param sesTok Session token.
     */
    @Override public void sessionToken(byte[] sesTok) {
        this.sesTok = sesTok;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, sesTok);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sesTok = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientAbstractMessage.class, this, super.toString());
    }
}